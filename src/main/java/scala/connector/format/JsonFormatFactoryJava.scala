package scala.connector.format

import java.util
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ReadableConfig}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType

class JsonFormatFactoryJava extends DeserializationFormatFactory{

  def createDecodingFormat(context: DynamicTableFactory.Context, formatOptions: ReadableConfig): DecodingFormat[DeserializationSchema[RowData]] = {
    new DecodingFormat[DeserializationSchema[RowData]]{
      def createRuntimeDecoder(context: DynamicTableSource.Context, producedDataType: DataType): DeserializationSchema[RowData] = {
        val rowType = producedDataType.getLogicalType.asInstanceOf[RowType]
        val rowDataTypeInfo = context.createTypeInformation[RowData](producedDataType)
        new JsonRowDataSimpleDeserializationSchemaJava(rowType, rowDataTypeInfo)
      }

      def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly
    }
  }

  def factoryIdentifier(): String = "myjsonjava"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions
  }
}
