package scala.connector.socket

import java.util

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSourceFactory, FactoryUtil}

class SocketDynamicTableFactory extends DynamicTableSourceFactory{
  import SocketDynamicTableFactory._

  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    // 先调这个，helper.validate()不会报format的属性不存在
    val decodingFormat: DecodingFormat[DeserializationSchema[RowData]] = helper.discoverDecodingFormat(
      classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)

    helper.validate()

    /**
     * 不过没事，KafkaDynamicTableFactory也调用的这个，内部调用了新的没废弃的方法
     * deprecated: org.apache.flink.table.catalog.ResolvedCatalogBaseTable.getSchema
     */
    val producedDataType = context.getCatalogTable.getSchema.toPhysicalRowDataType

    val host = config.get(HOSTNAME)
    val port = config.get(PORT)

    new SocketDynamicTableSource(host, port, decodingFormat, producedDataType)
  }

  override def factoryIdentifier(): String = "mysocket"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(HOSTNAME)
    requiredOptions.add(PORT)
    requiredOptions.add(FactoryUtil.FORMAT) // use pre-defined option for format
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions
  }
}

object SocketDynamicTableFactory{
  val HOSTNAME = ConfigOptions.key("hostname").stringType.noDefaultValue
  val PORT = ConfigOptions.key("port").intType.noDefaultValue
}