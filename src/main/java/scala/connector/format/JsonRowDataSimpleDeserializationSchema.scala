package scala.connector.format

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory, JsonFactoryBuilder, JsonParser, JsonToken}
import com.fasterxml.jackson.core.JsonToken.{VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING}
import com.fasterxml.jackson.core.json.JsonReadFeature
import java.io.{ByteArrayOutputStream, Closeable}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, CHAR, DOUBLE, FLOAT, INTEGER, VARCHAR}
import org.apache.flink.table.types.logical.RowType
import scala.collection.JavaConverters._

class JsonRowDataSimpleDeserializationSchema(
  rowType: RowType,
  resultTypeInfo: TypeInformation[RowData]
) extends DeserializationSchema[RowData]{
    @transient var parser: Array[Byte] => RowData = _
    @transient var fieldConverters: Array[JsonParser => AnyRef] = _
    @transient var names: java.util.Map[String, Integer] = _
    @transient var jsonFactory: JsonFactory = _
    @transient var nullRow: GenericRowData = _
    @transient var row: GenericRowData = _

    override def open(context: DeserializationSchema.InitializationContext): Unit = {
        names = new java.util.HashMap[String, Integer]
        rowType.getFields.asScala.map(_.getName).zipWithIndex.foreach{case (name, i) => names.put(name, i)}
        fieldConverters = rowType.getFields.asScala.map{ field =>
            val fieldConverter: JsonParser => AnyRef = field.getType.getTypeRoot match {
                case CHAR | VARCHAR =>
                    (parser: JsonParser) => {
                        parser.getCurrentToken match {
                            case VALUE_STRING => StringData.fromString(parser.getText)
                            case _ =>
                                val writer = new ByteArrayOutputStream()
                                tryWithResource(jsonFactory.createGenerator(writer, JsonEncoding.UTF8)) {
                                    generator => generator.copyCurrentStructure(parser)
                                }
                                StringData.fromBytes(writer.toByteArray)
                        }

                    }
                case INTEGER =>
                    (parser: JsonParser) => {
                        parser.getCurrentToken match {
                            case VALUE_NUMBER_INT => parser.getIntValue:Integer
                            case VALUE_STRING => parser.getText.toInt:Integer
                        }
                    }
                case BIGINT =>
                    (parser: JsonParser) => {
                        parser.getCurrentToken match {
                            case VALUE_NUMBER_INT => parser.getLongValue:java.lang.Long
                            case VALUE_STRING => parser.getText.toLong:java.lang.Long
                        }
                    }
                case FLOAT =>
                    (parser: JsonParser) => {
                        parser.getCurrentToken match {
                            case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT => parser.getFloatValue:java.lang.Float
                            case VALUE_STRING => parser.getText.toFloat:java.lang.Float
                        }
                    }
                case DOUBLE =>
                    (parser: JsonParser) => {
                        parser.getCurrentToken match {
                            case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT => parser.getDoubleValue:java.lang.Double
                            case VALUE_STRING => parser.getText.toDouble:java.lang.Double
                        }
                    }
            }
            fieldConverter
        }.toArray
        jsonFactory = new JsonFactoryBuilder()
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
                .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
                .build()
        nullRow = new GenericRowData(rowType.getFields.size)
        row = new GenericRowData(rowType.getFields.size)
    }

    override def deserialize(message: Array[Byte]): RowData = {
        val parser = jsonFactory.createParser(message)
        try {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return nullRow
            }
            var i = 0
            while (i < names.size){
                row.setField(i, null)
                i += 1
            }

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.getCurrentToken == JsonToken.FIELD_NAME) {
                    val jsonField = parser.getCurrentName
                    val index = names.get(jsonField)
                    if (index != null) {
                        if (parser.nextToken() != JsonToken.VALUE_NULL) {
                            row.setField(index, fieldConverters(index).apply(parser))
                        }
                    }
                }
                parser.skipChildren()
            }
        } catch {
            case e:Exception =>
        } finally {
            parser.close()
        }

        row
    }

    def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
        val resource = createResource
        try f.apply(resource) finally resource.close()
    }

    override def isEndOfStream(nextElement: RowData): Boolean = false

    override def getProducedType: TypeInformation[RowData] = resultTypeInfo
}
