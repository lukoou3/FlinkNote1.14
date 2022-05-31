package scala.connector.localfile

import java.util

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.format.{DecodingFormat, EncodingFormat}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil, SerializationFormatFactory}

class LocalFileTableFactory extends DynamicTableSourceFactory with DynamicTableSinkFactory{
  import LocalFileTableFactory._

  def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
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

    val path = config.get(PATH)
    val sleep = config.get(SOURCE_SLEEP)
    val numberOfRowsForSubtask = config.get(NUMBER_OF_ROWS)

    new LocalFileTableSource(path, sleep, numberOfRowsForSubtask, decodingFormat, producedDataType)
  }

  def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    assert(config.get(FactoryUtil.FORMAT) == "json" || config.get(FactoryUtil.FORMAT) == "csv")

    val encodingFormat: EncodingFormat[SerializationSchema[RowData]] = helper.discoverEncodingFormat(classOf[SerializationFormatFactory], FactoryUtil.FORMAT)

    helper.validate()

    //val physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType()
    val physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()

    new LocalFileTableSink(config.get(PATH), encodingFormat, physicalDataType)
  }

  def factoryIdentifier(): String = "mylocalfile"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(PATH)
    requiredOptions.add(FactoryUtil.FORMAT) // use pre-defined option for format
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions.add(SOURCE_SLEEP)
    optionalOptions.add(NUMBER_OF_ROWS)
    optionalOptions
  }

}

object LocalFileTableFactory{
  val PATH = ConfigOptions.key("path").stringType.noDefaultValue
  val SOURCE_SLEEP  = ConfigOptions.key("source.sleep").longType().defaultValue(10L)
  val NUMBER_OF_ROWS  = ConfigOptions.key("number-of-rows-subtask").longType().defaultValue(Long.MaxValue)
}