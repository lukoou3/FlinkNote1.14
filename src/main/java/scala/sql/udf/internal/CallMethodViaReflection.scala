package scala.sql.udf.internal

import java.lang.reflect.{Method, Modifier}

import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.StringData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.LogicalTypeRoot
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import InternalScalarFunction._
import scala.annotation.varargs
import scala.log.Logging

/**
 * An expression that invokes a method on a class via reflection.
 *
 * For now, only types defined in `Reflect.typeMapping` are supported (basically primitives
 * and string) as input types, and the output is turned automatically to a string.
 *
 * Note that unlike Hive's reflect function, this expression calls only static methods
 * (i.e. does not support calling non-static methods).
 *
 * param children the first element should be a literal string for the class name,
 *                 and the second element should be a literal string for the method name,
 *                 and the remaining are input arguments to the Java method.
 */
class CallMethodViaReflection extends InternalScalarFunction with Logging{
  var className: String = null
  var methodName: String = null
  var argTypes: Seq[DataType] = null

  @varargs
  def eval(elems: AnyRef*): StringData = {
    var i = 0
    while (i < argTypes.length) {
      buffer(i) = elems(i + 2)
      // Convert if necessary. Based on the types defined in typeMapping, string is the only
      // type that needs conversion. If we support timestamps, dates, decimals, arrays, or maps
      // in the future, proper conversion needs to happen here too.
      if (buffer(i).isInstanceOf[StringData]) {
        buffer(i) = buffer(i).toString
      }
      i += 1
    }
    try{
      val ret = method.invoke(null, buffer : _*)
      StringData.fromString(if(ret!= null) String.valueOf(ret) else null)
    }catch{
      case e: Throwable =>
        logError(s"call $method failed for ${buffer.mkString(", ")}", e)
        null
    }
  }

  /** True if the class exists and can be loaded. */
  @transient private lazy val classExists = CallMethodViaReflection.classExists(className)

  /** The reflection method. */
  @transient lazy val method: Method = {
    CallMethodViaReflection.findMethod(className, methodName, argTypes.map(_.getLogicalType.getTypeRoot)).orNull
  }

  /** A temporary buffer used to hold intermediate results returned by children. */
  @transient private lazy val buffer = new Array[Object](argTypes.length)

  override def argumentCount: ArgumentCount = minArgumentCount(2)

  override def stringArgs: Seq[String] = Seq("class", "method", "arg1", "...",  "argn")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    args.zipWithIndex.map{ case (tpe, i) =>  if(i <= 1) stringDateType else tpe}
  }

  override def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    if(!(0 until 2).forall(x => callContext.isArgumentLiteral(x))){
      throw callContext.newValidationError("first two arguments should be string literals")
    }

    className = callContext.getArgumentValue(0, classOf[String]).get()
    methodName = callContext.getArgumentValue(1, classOf[String]).get()
    argTypes = args.slice(2, args.length)

    if (!classExists) {
      throw callContext.newValidationError(s"class $className not found")
    }

    if (args.slice(2, args.length)
      .exists(e => !CallMethodViaReflection.typeMapping.contains(e.getLogicalType.getTypeRoot))) {
      throw callContext.newValidationError("arguments from the third require boolean, byte, short, " +
        "integer, long, float, double or string expressions")
    }

    if(method == null) {
      throw callContext.newValidationError(s"cannot find a static method that matches the argument types in $className for $argTypes")
    }

    stringDateType
  }
}

object CallMethodViaReflection{
  /** Mapping from Flink's type to acceptable JVM types. */
  val typeMapping = Map[LogicalTypeRoot, Seq[Class[_]]](
    BOOLEAN -> Seq(classOf[java.lang.Boolean], classOf[Boolean]),
    INTEGER -> Seq(classOf[java.lang.Integer], classOf[Int]),
    BIGINT -> Seq(classOf[java.lang.Long], classOf[Long]),
    FLOAT -> Seq(classOf[java.lang.Float], classOf[Float]),
    DOUBLE -> Seq(classOf[java.lang.Double], classOf[Double]),
    CHAR -> Seq(classOf[String]),
    VARCHAR -> Seq(classOf[String])
  )

  /**
   * Returns true if the class can be found and loaded.
   */
  private def classExists(className: String): Boolean = {
    try {
      Class.forName(className)
      true
    } catch {
      case e: ClassNotFoundException => false
    }
  }

  /**
   * Finds a Java static method using reflection that matches the given argument types,
   * and whose return type is string.
   *
   * The types sequence must be the valid types defined in [[typeMapping]].
   *
   * This is made public for unit testing.
   */
  def findMethod(className: String, methodName: String, argTypes: Seq[LogicalTypeRoot]): Option[Method] = {
    val clazz: Class[_] = Class.forName(className)
    clazz.getMethods.find { method =>
      val candidateTypes = method.getParameterTypes
      if (method.getName != methodName) {
        // Name must match
        false
      } else if (!Modifier.isStatic(method.getModifiers)) {
        // Method must be static
        false
      } else if (candidateTypes.length != argTypes.length) {
        // Argument length must match
        false
      } else {
        // Argument type must match. That is, either the method's argument type matches one of the
        // acceptable types defined in typeMapping, or it is a super type of the acceptable types.
        candidateTypes.zip(argTypes).forall { case (candidateType, argType) =>
          typeMapping(argType).exists(candidateType.isAssignableFrom)
        }
      }
    }
  }
}