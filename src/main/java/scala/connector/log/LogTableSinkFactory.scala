package scala.connector.log

import java.util

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.format.EncodingFormat
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, FactoryUtil, SerializationFormatFactory}

class LogTableSinkFactory extends DynamicTableSinkFactory{
  import LogTableSinkFactory._
  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    assert(config.get(FactoryUtil.FORMAT) == "json" || config.get(FactoryUtil.FORMAT) == "csv")

    val encodingFormat: EncodingFormat[SerializationSchema[RowData]] = helper.discoverEncodingFormat(classOf[SerializationFormatFactory], FactoryUtil.FORMAT)

    helper.validate()

    //val physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType()
    val physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()

    new LogTableSink(
      config.get(LOG_LEVEL),
      encodingFormat,
      physicalDataType,
      config.getOptional(FactoryUtil.SINK_PARALLELISM)
    )
  }

  override def factoryIdentifier(): String = "mylog"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(LOG_LEVEL)
    requiredOptions.add(FactoryUtil.FORMAT)
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions.add(FactoryUtil.SINK_PARALLELISM)
    optionalOptions
  }
}

object LogTableSinkFactory{
  val LOG_LEVEL = ConfigOptions.key("log-level").stringType.noDefaultValue
}