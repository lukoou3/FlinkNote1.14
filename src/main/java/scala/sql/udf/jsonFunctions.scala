package scala.sql.udf

import java.util.Optional

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{CallContext, TypeInference, TypeStrategy}

import scala.util.parsing.combinator.RegexParsers

private[this] sealed trait PathInstruction
private[this] object PathInstruction {
  case object Subscript extends PathInstruction
  case object Wildcard extends PathInstruction
  case object Key extends PathInstruction
  case class Index(index: Int) extends PathInstruction
  case class Named(name: String) extends PathInstruction
}

private[this] object JsonPathParser extends RegexParsers {
  import PathInstruction._

  def root: Parser[Char] = '$'

  def int: Parser[Int] = "\\d+".r ^? {
    case x => x.toInt
  }

  // parse `[*]` and `[123]` subscripts
  def subscript: Parser[List[PathInstruction]] =
    for {
      operand <- '[' ~> ('*' ^^^ Wildcard | int ^^ Index) <~ ']'
    } yield {
      Subscript :: operand :: Nil
    }

  // parse `.name` or `['name']` child expressions
  def named: Parser[List[PathInstruction]] =
    for {
      name <- '.' ~> "[^\\.\\[]+".r | "['" ~> "[^\\'\\?]+".r <~ "']"
    } yield {
      Key :: Named(name) :: Nil
    }

  // child wildcards: `..`, `.*` or `['*']`
  def wildcard: Parser[List[PathInstruction]] =
    (".*" | "['*']") ^^^ List(Wildcard)

  def node: Parser[List[PathInstruction]] =
    wildcard |
      named |
      subscript

  val expression: Parser[List[PathInstruction]] = {
    phrase(root ~> rep(node) ^^ (x => x.flatten))
  }

  def parse(str: String): Option[List[PathInstruction]] = {
    this.parseAll(expression, str) match {
      case Success(result, _) =>
        Some(result)

      case _ =>
        None
    }
  }
}

class GetJsonObject extends ScalarFunction {
  @transient private var parsedPath: Option[List[PathInstruction]] = null
  var valuePath:String = null

  override def open(context: FunctionContext): Unit = {
    assert(valuePath != null)
  }

  def eval(json: String, path: String): String ={
    if(json == null){
      return null
    }

    if(parsedPath == null){
      assert(valuePath == path)
      parsedPath = parsePath(path)
    }

    if (parsedPath.isDefined) {
      try {
        parse(JSON.parse(json), parsedPath.get)
      } catch {
        case _: Exception => null
      }
    } else {
      null
    }
  }

  def parse(obj: Object, path: List[PathInstruction]): String ={
    import PathInstruction._
    (obj, path) match {
      case (null, _) => null
      case (o, Nil) => if(o == null) null else o.toString
      case (map: JSONObject, Key :: Named(name) :: xs) => parse(map.get(name), xs)
      case (list: JSONArray, Subscript :: Index(idx) :: xs) => parse(if(idx < list.size()) list.get(idx) else null, xs)
      case _ => null
    }
  }

  private def parsePath(path: String): Option[List[PathInstruction]] = {
    if (path != null) {
      JsonPathParser.parse(path)
    } else {
      None
    }
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes
          if (argumentDataTypes.size() != 2) {
            throw callContext.newValidationError(s"input to function explode should be array or row type, not $argumentDataTypes")
          }
          if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
            throw callContext.newValidationError("Literal expected for second argument.");
          }

          valuePath = callContext.getArgumentValue(1, classOf[String]).get()
          Optional.of(DataTypes.STRING())
        }
      })
      .build()
  }
}


