package scala.sql.udf.internal

import java.io.{ByteArrayOutputStream, CharArrayWriter, StringWriter}

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactoryBuilder, JsonGenerator, JsonParser, JsonToken}
import com.fasterxml.jackson.core.json.JsonReadFeature
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{ArrayData, GenericRowData, MapData, RowData, StringData}
import org.apache.flink.table.functions.FunctionContext
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot.{ARRAY, MAP, ROW}

import scala.util.parsing.combinator.RegexParsers
import InternalScalarFunction._
import scala.annotation.varargs
import scala.sql.json.{CreateJacksonParser, JacksonGenerator, JacksonParser}
import scala.util.Utils

private[this] sealed trait PathInstruction
private[this] object PathInstruction {
  case object Subscript extends PathInstruction
  case object Wildcard extends PathInstruction
  case object Key extends PathInstruction
  case class Index(index: Long) extends PathInstruction
  case class Named(name: String) extends PathInstruction
}

private[this] sealed trait WriteStyle
private[this] object WriteStyle {
  case object RawStyle extends WriteStyle
  case object QuotedStyle extends WriteStyle
  case object FlattenStyle extends WriteStyle
}

private[this] object JsonPathParser extends RegexParsers {
  import PathInstruction._

  def root: Parser[Char] = '$'

  def long: Parser[Long] = "\\d+".r ^? {
    case x => x.toLong
  }

  // parse `[*]` and `[123]` subscripts
  def subscript: Parser[List[PathInstruction]] =
    for {
      operand <- '[' ~> ('*' ^^^ Wildcard | long ^^ Index) <~ ']'
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

private[this] object SharedFactory {
  val jsonFactory = new JsonFactoryBuilder()
    // The two options below enabled for Hive compatibility
    .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
    .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
    .build()
}

class GetJsonObject extends InternalScalarFunction{
  import com.fasterxml.jackson.core.JsonToken._

  import PathInstruction._
  import SharedFactory._
  import WriteStyle._

  var pathFoldable = false
  @transient private var parsedPath: Option[List[PathInstruction]] = null

  def eval(jsonStr: StringData, path: StringData): StringData ={
    if(jsonStr == null){
      return null
    }

    val parsed = if (pathFoldable) {
      if(parsedPath == null){
        parsedPath = parsePath(path)
      }
      parsedPath
    } else {
      parsePath(path)
    }

    if (parsedPath.isDefined) {
      /* We know the bytes are UTF-8 encoded. Pass a Reader to avoid having Jackson
                detect character encoding which could fail for some malformed strings */
      Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, jsonStr)) { parser =>
        val output = new ByteArrayOutputStream()
        val matched = Utils.tryWithResource(
          jsonFactory.createGenerator(output, JsonEncoding.UTF8)) { generator =>
          parser.nextToken()
          evaluatePath(parser, generator, RawStyle, parsed.get)
        }
        if (matched) {
          StringData.fromBytes(output.toByteArray)
        } else {
          null
        }
      }
    } else {
      null
    }
  }

  private def parsePath(path: StringData): Option[List[PathInstruction]] = {
    if (path != null) {
      JsonPathParser.parse(path.toString)
    } else {
      None
    }
  }

  // advance to the desired array index, assumes to start at the START_ARRAY token
  private def arrayIndex(p: JsonParser, f: () => Boolean): Long => Boolean = {
    case _ if p.getCurrentToken == END_ARRAY =>
      // terminate, nothing has been written
      false

    case 0 =>
      // we've reached the desired index
      val dirty = f()

      while (p.nextToken() != END_ARRAY) {
        // advance the token stream to the end of the array
        p.skipChildren()
      }

      dirty

    case i if i > 0 =>
      // skip this token and evaluate the next
      p.skipChildren()
      p.nextToken()
      arrayIndex(p, f)(i - 1)
  }

  /**
   * Evaluate a list of JsonPath instructions, returning a bool that indicates if any leaf nodes
   * have been written to the generator
   */
  private def evaluatePath(
    p: JsonParser,
    g: JsonGenerator,
    style: WriteStyle,
    path: List[PathInstruction]): Boolean = {
    (p.getCurrentToken, path) match {
      case (VALUE_STRING, Nil) if style == RawStyle =>
        // there is no array wildcard or slice parent, emit this string without quotes
        if (p.hasTextCharacters) {
          g.writeRaw(p.getTextCharacters, p.getTextOffset, p.getTextLength)
        } else {
          g.writeRaw(p.getText)
        }
        true

      case (START_ARRAY, Nil) if style == FlattenStyle =>
        // flatten this array into the parent
        var dirty = false
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, style, Nil)
        }
        dirty

      case (_, Nil) =>
        // general case: just copy the child tree verbatim
        g.copyCurrentStructure(p)
        true

      case (START_OBJECT, Key :: xs) =>
        var dirty = false
        while (p.nextToken() != END_OBJECT) {
          if (dirty) {
            // once a match has been found we can skip other fields
            p.skipChildren()
          } else {
            dirty = evaluatePath(p, g, style, xs)
          }
        }
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: Subscript :: Wildcard :: xs) =>
        // special handling for the non-structure preserving double wildcard behavior in Hive
        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, FlattenStyle, xs)
        }
        g.writeEndArray()
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: xs) if style != QuotedStyle =>
        // retain Flatten, otherwise use Quoted... cannot use Raw within an array
        val nextStyle = style match {
          case RawStyle => QuotedStyle
          case FlattenStyle => FlattenStyle
          case QuotedStyle => throw new IllegalStateException()
        }

        // temporarily buffer child matches, the emitted json will need to be
        // modified slightly if there is only a single element written
        val buffer = new StringWriter()

        var dirty = 0
        Utils.tryWithResource(jsonFactory.createGenerator(buffer)) { flattenGenerator =>
          flattenGenerator.writeStartArray()

          while (p.nextToken() != END_ARRAY) {
            // track the number of array elements and only emit an outer array if
            // we've written more than one element, this matches Hive's behavior
            dirty += (if (evaluatePath(p, flattenGenerator, nextStyle, xs)) 1 else 0)
          }
          flattenGenerator.writeEndArray()
        }

        val buf = buffer.getBuffer
        if (dirty > 1) {
          g.writeRawValue(buf.toString)
        } else if (dirty == 1) {
          // remove outer array tokens
          g.writeRawValue(buf.substring(1, buf.length()-1))
        } // else do not write anything

        dirty > 0

      case (START_ARRAY, Subscript :: Wildcard :: xs) =>
        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          // wildcards can have multiple matches, continually update the dirty count
          dirty |= evaluatePath(p, g, QuotedStyle, xs)
        }
        g.writeEndArray()

        dirty

      case (START_ARRAY, Subscript :: Index(idx) :: (xs@Subscript :: Wildcard :: _)) =>
        p.nextToken()
        // we're going to have 1 or more results, switch to QuotedStyle
        arrayIndex(p, () => evaluatePath(p, g, QuotedStyle, xs))(idx)

      case (START_ARRAY, Subscript :: Index(idx) :: xs) =>
        p.nextToken()
        arrayIndex(p, () => evaluatePath(p, g, style, xs))(idx)

      case (FIELD_NAME, Named(name) :: xs) if p.getCurrentName == name =>
        // exact field match
        if (p.nextToken() != JsonToken.VALUE_NULL) {
          evaluatePath(p, g, style, xs)
        } else {
          false
        }

      case (FIELD_NAME, Wildcard :: xs) =>
        // wildcard field match
        p.nextToken()
        evaluatePath(p, g, style, xs)

      case _ =>
        p.skipChildren()
        false
    }
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(2)

  override def stringArgs: Seq[String] = Seq("jsonStr", "path")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = Seq(stringDateType, stringDateType)

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    pathFoldable = callContext.isArgumentLiteral(1)
    stringDateType
  }
}

class JsonTuple extends InternalTableFunction{
  import SharedFactory._
  var fieldNames: Seq[String] = _
  @transient private lazy val nullRow = new GenericRowData(fieldNames.size)

  @varargs
  def eval(elems: AnyRef*): Unit = {
    val jsonStr = if(elems(0) != null) elems(0).asInstanceOf[StringData] else null
    if(jsonStr == null){
      collect(nullRow)
    }

    try {
      /* We know the bytes are UTF-8 encoded. Pass a Reader to avoid having Jackson
      detect character encoding which could fail for some malformed strings */
      Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, jsonStr)) { parser =>
        collect(parseRow(parser))
      }
    } catch {
      case _: Exception =>
        collect(nullRow)
    }
  }

  private def parseRow(parser: JsonParser): RowData = {
    // only objects are supported
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      return nullRow
    }

    //val row = Array.ofDim[AnyRef](fieldNames.length)
    val row = new GenericRowData(fieldNames.length)

    // start reading through the token stream, looking for any requested field names
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      if (parser.getCurrentToken == JsonToken.FIELD_NAME) {
        // check to see if this field is desired in the output
        val jsonField = parser.getCurrentName
        var idx = fieldNames.indexOf(jsonField)
        if (idx >= 0) {
          // it is, copy the child tree to the correct location in the output row
          val output = new ByteArrayOutputStream()

          // write the output directly to UTF8 encoded byte array
          if (parser.nextToken() != JsonToken.VALUE_NULL) {
            Utils.tryWithResource(jsonFactory.createGenerator(output, JsonEncoding.UTF8)) {
              generator => copyCurrentStructure(generator, parser)
            }

            val jsonValue = StringData.fromBytes(output.toByteArray)

            row.setField(idx, jsonValue)
            // SPARK-21804: json_tuple returns null values within repeated columns
            // except the first one; so that we need to check the remaining fields.
            /*do {
              row(idx) = jsonValue
              idx = fieldNames.indexOf(jsonField, idx + 1)
            } while (idx >= 0)*/
          }
        }
      }

      // always skip children, it's cheap enough to do even if copyCurrentStructure was called
      parser.skipChildren()
    }

    row
  }

  private def copyCurrentStructure(generator: JsonGenerator, parser: JsonParser): Unit = {
    parser.getCurrentToken match {
      // if the user requests a string field it needs to be returned without enclosing
      // quotes which is accomplished via JsonGenerator.writeRaw instead of JsonGenerator.write
      case JsonToken.VALUE_STRING if parser.hasTextCharacters =>
        // slight optimization to avoid allocating a String instance, though the characters
        // still have to be decoded... Jackson doesn't have a way to access the raw bytes
        generator.writeRaw(parser.getTextCharacters, parser.getTextOffset, parser.getTextLength)

      case JsonToken.VALUE_STRING =>
        // the normal String case, pass it through to the output without enclosing quotes
        generator.writeRaw(parser.getText)

      case JsonToken.VALUE_NULL =>
        // a special case that needs to be handled outside of this method.
        // if a requested field is null, the result must be null. the easiest
        // way to achieve this is just by ignoring null tokens entirely
        throw new IllegalStateException("Do not attempt to copy a null field")

      case _ =>
        // handle other types including objects, arrays, booleans and numbers
        generator.copyCurrentStructure(parser)
    }
  }

  override def argumentCount: ArgumentCount = anyArgumentCount

  override def stringArgs: Seq[String] = Seq("jsonStr", "p1", "p2", "...",  "pn")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    args.map(_ => DataTypes.STRING())
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    val fieldIdxs = (1 until args.length).toArray

    if(!fieldIdxs.forall(x => callContext.isArgumentLiteral(x))){
      throw callContext.newValidationError("属性参数必须是字面量")
    }

    fieldNames = fieldIdxs.map(i => callContext.getArgumentValue(i, classOf[String]).get())
    if(fieldNames.toSet.size != fieldNames.size){
      throw callContext.newValidationError("属性参数不能重复")
    }

    val fields = fieldNames.map(name => DataTypes.FIELD(name, DataTypes.STRING()))
    DataTypes.ROW(fields:_*)
  }
}

class StructsToJson extends InternalScalarFunction{
  self =>
  var logicalType: LogicalType = _
  @transient
  lazy val writer = new CharArrayWriter()
  @transient
  lazy val gen = new JacksonGenerator(logicalType, writer)
  // This converts rows to the JSON output according to the given schema.
  @transient
  lazy val converter: Any => StringData = {
    def getAndReset(): StringData = {
      gen.flush()
      val json = writer.toString
      writer.reset()
      StringData.fromString(json)
    }

    logicalType.getTypeRoot match {
      case ROW =>
        (row: Any) =>
          gen.write(row.asInstanceOf[RowData])
          getAndReset()
      case ARRAY =>
        (arr: Any) =>
          gen.write(arr.asInstanceOf[ArrayData])
          getAndReset()
      case MAP =>
        (map: Any) =>
          gen.write(map.asInstanceOf[MapData])
          getAndReset()
    }
  }

  def eval(value: Object): StringData = {
    if (value == null) {
      null
    } else {
      converter(value)
    }
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(1)

  override def stringArgs: Seq[String] = Seq("value")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    val dType = args(0).getLogicalType.getTypeRoot
    if(dType !=  ROW && dType !=  ARRAY && dType !=  MAP){
      throw callContext.newValidationError(s"input to function explode should be array or row type, not $dType")
    }
    args
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    self.logicalType = args(0).getLogicalType
    DataTypes.STRING()
  }
}

class JsonToStructs extends InternalScalarFunction{
  self =>
  var logicalType: LogicalType = _
  @transient var parser: StringData => Seq[RowData] = _
  @transient var converter: Seq[RowData] => AnyRef = _

  override def open(context: FunctionContext): Unit = {
    val rawParser = new JacksonParser(logicalType)
    val createParser = CreateJacksonParser.utf8String _
    parser = str => rawParser.parse[StringData](str, createParser)
    converter = logicalType.getTypeRoot match {
      case ROW => row => row.head
      case ARRAY => row => row.head.getArray(0)
      case MAP => row => row.head.getMap(0)
    }
  }

  def eval(jsonStr: StringData, schema: StringData): AnyRef = {
    if(jsonStr == null){
      null
    }else{
      converter(parser(jsonStr))
    }
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(2)

  override def stringArgs: Seq[String] = Seq("jsonStr", "schema")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = Seq(stringDateType, stringDateType)

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
      throw callContext.newValidationError("Literal expected for second argument.");
    }

    val dataType = typeFactory.createDataType(callContext.getArgumentValue(1, classOf[String]).get())
    self.logicalType = dataType.getLogicalType

    dataType
  }
}