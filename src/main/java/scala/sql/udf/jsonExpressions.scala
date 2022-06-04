package scala.sql.udf

import java.io.CharArrayWriter
import java.util
import java.util.{Collections, Optional}

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.StringData
import org.apache.flink.table.functions.{FunctionContext, FunctionDefinition, ScalarFunction}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot.{ARRAY, MAP, ROW}
import org.apache.flink.table.data.{ArrayData, MapData, RowData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.Signature.Argument
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext, ConstantArgumentCount, InputTypeStrategy, Signature, TypeInference, TypeStrategy}

import scala.sql.json.{CreateJacksonParser, JacksonGenerator, JacksonParser}

class StructsToJson extends ScalarFunction {
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

  override def open(context: FunctionContext): Unit = {
  }

  def eval(value: Object): StringData = {
    if (value == null) {
      null
    } else {
      converter(value)
    }
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .inputTypeStrategy(
        new InputTypeStrategy{
          override def getArgumentCount: ArgumentCount = ConstantArgumentCount.of(1)

          override def inferInputTypes(callContext: CallContext, throwOnFailure: Boolean): Optional[util.List[DataType]] = {
            val args: util.List[DataType] = callContext.getArgumentDataTypes()
            val arg0: DataType = args.get(0)
            val claszz =  arg0.getLogicalType().getTypeRoot() match {
              case ROW => classOf[RowData]
              case ARRAY => classOf[ArrayData]
              case MAP => classOf[MapData]
            }
            Optional.of(util.Arrays.asList(arg0.bridgedTo(claszz)))
          }

          override def getExpectedSignatures(definition: FunctionDefinition): util.List[Signature] = {
            Collections.singletonList(Signature.of(Argument.of("value")))
          }
        }
      )
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes
          if (argumentDataTypes.size() != 1) {
            throw callContext.newValidationError(s"input to function explode should be array or row type, not $argumentDataTypes")
          }

          val logicalType: LogicalType = argumentDataTypes.get(0).getLogicalType
          if (logicalType.getTypeRoot != ARRAY && logicalType.getTypeRoot != ROW && logicalType.getTypeRoot != MAP) {
            throw callContext.newValidationError(s"input to function explode should be array or row type, not $argumentDataTypes")
          }
          self.logicalType = logicalType

          Optional.of(DataTypes.STRING().bridgedTo(classOf[StringData]))
        }
      })
      .build()
  }
}

class JsonToStructs extends ScalarFunction {
  self =>
  var logicalType: LogicalType = _
  @transient var parser: StringData => Seq[RowData] = _
  @transient var converter: Seq[RowData] => AnyRef = _

  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }

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

  def eval(str: StringData, schema: StringData): AnyRef = {
    if(str == null){
      null
    }else{
      converter(parser(str))
    }
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .inputTypeStrategy(
        new InputTypeStrategy{
          override def getArgumentCount: ArgumentCount = ConstantArgumentCount.of(2)

          override def inferInputTypes(callContext: CallContext, throwOnFailure: Boolean): Optional[util.List[DataType]] = {
            val args: util.List[DataType] = callContext.getArgumentDataTypes()
            val arg0: DataType = args.get(0)
            val arg1: DataType = args.get(1)
            Optional.of(util.Arrays.asList(arg0.bridgedTo(classOf[StringData]), arg1.bridgedTo(classOf[StringData])))
          }

          override def getExpectedSignatures(definition: FunctionDefinition): util.List[Signature] = {
            Collections.singletonList(Signature.of(Argument.of("json"), Argument.of("schema")))
          }
        }
      )
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes
          if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
            throw callContext.newValidationError("Literal expected for second argument.");
          }

          val dataType = typeFactory.createDataType(callContext.getArgumentValue(1, classOf[String]).get())
          self.logicalType = dataType.getLogicalType

          val claszz =  dataType.getLogicalType().getTypeRoot() match {
            case ROW => classOf[RowData]
            case ARRAY => classOf[ArrayData]
            case MAP => classOf[MapData]
          }

          Optional.of(dataType.bridgedTo(claszz))
        }
      })
      .build()
  }
}