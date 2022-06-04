package scala.sql.udf

import java.util
import java.util.{Collections, Optional}

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.functions.{FunctionDefinition, ScalarFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.Signature.Argument
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext, ConstantArgumentCount, InputTypeStrategy, Signature, TypeInference, TypeStrategy}
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import scala.annotation.varargs
import scala.collection.JavaConverters._

class CreateNamedStruct extends ScalarFunction {
  var size = 0

  @varargs
  def eval(elems: AnyRef*): RowData = {
    val row = new GenericRowData(size)
    var i = 0
    while(2*i + 1 < elems.length){
      row.setField(i, elems(2*i + 1))
      i += 1
    }
    row
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .inputTypeStrategy(
        new InputTypeStrategy{
          override def getArgumentCount: ArgumentCount = ConstantArgumentCount.any()

          override def inferInputTypes(callContext: CallContext, throwOnFailure: Boolean): Optional[util.List[DataType]] = {
            val types = callContext.getArgumentDataTypes().asScala.map{dType =>
              val claszz = dataTypeConversionClass(dType)
              dType.bridgedTo(claszz)
            }.asJava

            Optional.of(types)
          }

          override def getExpectedSignatures(definition: FunctionDefinition): util.List[Signature] = {
            "name1, val1, name2, val2, ...".split(",").map(_.trim)
              .map(x => Signature.of(Argument.of(x))).toBuffer.asJava
          }
        }
      )
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes.asScala
          if(argumentDataTypes.isEmpty || argumentDataTypes.length % 2 == 1 ||
            !(0 until argumentDataTypes.length by 2).forall(callContext.isArgumentLiteral(_))){
            throw callContext.newValidationError("Arguments cant not.")
          }

          size = argumentDataTypes.length / 2

          val names = (0 until argumentDataTypes.length by 2).map(callContext.getArgumentValue(_, classOf[String]).get())

          val values = argumentDataTypes.grouped(2).map {
            case Seq(name, value) => value
          }.toSeq

          val fields = names.zip(values).map{ case (name, value) => DataTypes.FIELD(name, value)}

          Optional.of(DataTypes.ROW(fields:_*).bridgedTo(classOf[RowData]))
        }
      })
      .build()
  }

  def dataTypeConversionClass(dType: DataType): Class[_] ={
    dType.getLogicalType.getTypeRoot match {
      case CHAR | VARCHAR => classOf[StringData]
      case INTEGER => classOf[Integer]
      case BIGINT => classOf[java.lang.Long]
      case FLOAT => classOf[java.lang.Float]
      case DOUBLE => classOf[java.lang.Double]
      case TIME_WITHOUT_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE => classOf[TimestampData]
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${dType.getLogicalType.getTypeRoot}")
    }
  }

}
