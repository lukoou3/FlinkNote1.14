package scala.connector.format

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.data.{RowData, StringData}
import org.apache.flink.table.types.logical.RowType

class JsonRowDataDeserializationSchema(
  rowType: RowType,
  resultTypeInfo: TypeInformation[RowData]
) extends DeserializationSchema[RowData]{
    @transient var parser: Array[Byte] => RowData = _

    override def open(context: DeserializationSchema.InitializationContext): Unit = {
        super.open(context)
        val rawParser = new JacksonParser(rowType)
        val createParser = CreateJacksonParser.bytes _
        parser = str => rawParser.parse[Array[Byte]](str, createParser)
    }

    override def deserialize(message: Array[Byte]): RowData = {
        parser(message)
    }

    override def isEndOfStream(nextElement: RowData): Boolean = false

    override def getProducedType: TypeInformation[RowData] = resultTypeInfo
}
