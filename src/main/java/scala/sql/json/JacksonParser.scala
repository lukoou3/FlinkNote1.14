package scala.sql.json

import java.io.{ByteArrayOutputStream, CharConversionException, Closeable}
import java.nio.charset.MalformedInputException

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory, JsonParser, JsonProcessingException, JsonToken}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{ArrayType, LogicalType, MapType, RowType}
import org.apache.flink.table.data.{ArrayData, GenericArrayData, GenericRowData, MapData, RowData, StringData}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Constructs a parser for a given schema that translates a json string to an [[InternalRow]].
 */
class JacksonParser(logicalType: LogicalType) {
  import com.fasterxml.jackson.core.JsonToken._

  // A `ValueConverter` is responsible for converting a value from `JsonParser`
  // to a value in a field for `InternalRow`.
  private type ValueConverter = JsonParser => AnyRef

  // `ValueConverter`s for the root schema for all fields in the schema
  private val rootConverter = makeRootConverter(logicalType)

  private val factory = new JsonFactory()

  /**
   * Create a converter which converts the JSON documents held by the `JsonParser`
   * to a value according to a desired schema. This is a wrapper for the method
   * `makeConverter()` to handle a row wrapped with an array.
   */
  private def makeRootConverter(logicalType: LogicalType): JsonParser => Seq[RowData] = {
    logicalType match {
      case rowType: RowType => makeStructRootConverter(rowType)
      //case mt: MapType => makeMapRootConverter(mt)
      case at: ArrayType => makeArrayRootConverter(at)
    }
  }

  private def makeStructRootConverter(rowType: RowType): JsonParser => Seq[RowData] = {
    val elementConverter = makeConverter(rowType)
    val fieldConverters = rowType.getFields.asScala.map(_.getType).map(makeConverter).toArray
    val names = logicalType.asInstanceOf[RowType].getFields.asScala.map(_.getName).zipWithIndex.toMap
    (parser: JsonParser) => parseJsonToken[Seq[RowData]](parser, rowType) {
      case START_OBJECT => convertObject(parser, names, fieldConverters) :: Nil
      case START_ARRAY =>
        throw new RuntimeException("Parsing JSON arrays as structs is forbidden.")
    }
  }

  private def makeArrayRootConverter(at: ArrayType): JsonParser => Seq[RowData] = {
    val elementConverter = makeConverter(at.getElementType)
    (parser: JsonParser) => parseJsonToken[Seq[RowData]](parser, at) {
      case START_ARRAY => GenericRowData.of(convertArray(parser, elementConverter)) :: Nil
      case START_OBJECT =>
        throw new RuntimeException("Parsing JSON object as arrays is forbidden.")
    }
  }

  /**
   * Create a converter which converts the JSON documents held by the `JsonParser`
   * to a value according to a desired schema.
   */
  def makeConverter(logicalType: LogicalType): ValueConverter = logicalType.getTypeRoot match {
    case INTEGER =>
      (parser: JsonParser) => parseJsonToken[java.lang.Integer](parser, logicalType) {
        case VALUE_NUMBER_INT => parser.getIntValue
        // spark中没有这个判断
        case VALUE_STRING => parser.getText.toInt
      }

    case BIGINT =>
      (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, logicalType) {
        case VALUE_NUMBER_INT => parser.getLongValue
      }

    case FLOAT =>
      (parser: JsonParser) => parseJsonToken[java.lang.Float](parser, logicalType) {
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
          parser.getFloatValue

        case VALUE_STRING =>
          // Special case handling for NaN and Infinity.
          parser.getText match {
            case "NaN" => Float.NaN
            case "Infinity" => Float.PositiveInfinity
            case "-Infinity" => Float.NegativeInfinity
            case other => throw new RuntimeException(
              s"Cannot parse $other as $logicalType.")
          }
      }

    case DOUBLE =>
      (parser: JsonParser) => parseJsonToken[java.lang.Double](parser, logicalType) {
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
          parser.getDoubleValue

        case VALUE_STRING =>
          // Special case handling for NaN and Infinity.
          parser.getText match {
            case "NaN" => Double.NaN
            case "Infinity" => Double.PositiveInfinity
            case "-Infinity" => Double.NegativeInfinity
            case other =>
              throw new RuntimeException(s"Cannot parse $other as DoubleType.")
          }
      }

    case CHAR | VARCHAR =>
      (parser: JsonParser) => parseJsonToken[StringData](parser, logicalType) {
        case VALUE_STRING =>
          StringData.fromString(parser.getText)

        case _ =>
          // Note that it always tries to convert the data as string without the case of failure.
          val writer = new ByteArrayOutputStream()
          tryWithResource(factory.createGenerator(writer, JsonEncoding.UTF8)) {
            generator => generator.copyCurrentStructure(parser)
          }
          StringData.fromBytes(writer.toByteArray)
      }

    case ROW =>
      val fieldConverters = logicalType.asInstanceOf[RowType].getFields.asScala.map(_.getType).map(makeConverter).toArray
      val names = logicalType.asInstanceOf[RowType].getFields.asScala.map(_.getName).zipWithIndex.toMap
      (parser: JsonParser) => parseJsonToken[RowData](parser, logicalType) {
        case START_OBJECT => convertObject(parser, names, fieldConverters)
      }

    case ARRAY =>
      val elementConverter = makeConverter(logicalType.asInstanceOf[ArrayType].getElementType)
      (parser: JsonParser) => parseJsonToken[ArrayData](parser, logicalType) {
        case START_ARRAY => convertArray(parser, elementConverter)
      }
    case _ =>
      (parser: JsonParser) =>
        // Here, we pass empty `PartialFunction` so that this case can be
        // handled as a failed conversion. It will throw an exception as
        // long as the value is not null.
        null
        //parseJsonToken[AnyRef](parser, dataType)(PartialFunction.empty[JsonToken, AnyRef])
  }

  /**
   * This method skips `FIELD_NAME`s at the beginning, and handles nulls ahead before trying
   * to parse the JSON token using given function `f`. If the `f` failed to parse and convert the
   * token, call `failedConversion` to handle the token.
   */
  private def parseJsonToken[R >: Null](
    parser: JsonParser,
    logicalType: LogicalType)(f: PartialFunction[JsonToken, R]): R = {
    parser.getCurrentToken match {
      case FIELD_NAME =>
        // There are useless FIELD_NAMEs between START_OBJECT and END_OBJECT tokens
        parser.nextToken()
        parseJsonToken[R](parser, logicalType)(f)

      case null | VALUE_NULL => null

      case other => f.applyOrElse(other, failedConversion(parser, logicalType))
    }
  }

  /**
   * This function throws an exception for failed conversion, but returns null for empty string,
   * to guard the non string types.
   */
  private def failedConversion[R >: Null](
    parser: JsonParser,
    logicalType: LogicalType): PartialFunction[JsonToken, R] = {
    case VALUE_STRING if parser.getTextLength < 1 =>
      // If conversion is failed, this produces `null` rather than throwing exception.
      // This will protect the mismatch of types.
      null

    case token =>
      // We cannot parse this token based on the given data type. So, we throw a
      // RuntimeException and this exception will be caught by `parse` method.
      throw new RuntimeException(
        s"Failed to parse a value for data type $logicalType (current token: $token).")
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
    parser: JsonParser,
    names: Map[String, Int],
    fieldConverters: Array[ValueConverter]): RowData = {
    val row = new GenericRowData(names.size)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      names.get(parser.getCurrentName) match {
        case Some(index) =>
          row.setField(index, fieldConverters(index).apply(parser))
        case None =>
          parser.skipChildren()
      }
    }

    row
  }

  /**
   * Parse an object as a Array.
   */
  private def convertArray(
    parser: JsonParser,
    fieldConverter: ValueConverter): ArrayData = {
    val values = ArrayBuffer.empty[AnyRef]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += fieldConverter.apply(parser)
    }
    new GenericArrayData(values.toArray)
  }

  /**
   * Parse the JSON input to the set of [[InternalRow]]s.
   *
   */
  def parse[T](
    record: T,
    createParser: (JsonFactory, T) => JsonParser): Seq[RowData] = {
    try {
      tryWithResource(createParser(factory, record)) { parser =>
        // a null first token is equivalent to testing for input.trim.isEmpty
        // but it works on any token stream and not just strings
        parser.nextToken() match {
          case null => Nil
          case _ => rootConverter.apply(parser) match {
            case null => throw new RuntimeException("Root converter returned null")
            case rows => rows
          }
        }
      }
    } catch {
      case e @ (_: RuntimeException | _: JsonProcessingException | _: MalformedInputException) =>
        // JSON parser currently doesn't support partial results for corrupted records.
        // For such records, all fields other than the field configured by
        // `columnNameOfCorruptRecord` are set to `null`.
        throw new Exception()
    }
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  /**
   * Advance the parser until a null or a specific token is found
   */
  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }
}

object Test{
  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }

  def main(args: Array[String]): Unit = {
    val createParser = string _
    val rawParser = new JacksonParser(null)


  }
}