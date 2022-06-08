package scala.connector.jdbc

import java.time.Duration
import java.util

import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, FactoryUtil}

import JdbcTableFactory._

class JdbcTableFactory extends DynamicTableSinkFactory{

  def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    helper.validate()

    new JdbcTableSink(
      context.getCatalogTable().getResolvedSchema(),
      config.get(DB_NAME),
      config.get(TABLE_NAME) ,
      config.get(SINK_BATCH_SIZE),
      config.get(SINK_BATCH_INTERVAL).toMillis,
      config.get(SINK_MAX_RETRIES),
      config.get(SINK_UPDATE_MODE)
    )
  }

  def factoryIdentifier(): String = "myjdbc"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(DB_NAME)
    requiredOptions.add(TABLE_NAME)
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(SINK_BATCH_SIZE)
    requiredOptions.add(SINK_BATCH_INTERVAL)
    requiredOptions.add(SINK_MAX_RETRIES)
    requiredOptions.add(SINK_UPDATE_MODE)
    optionalOptions
  }
}

object JdbcTableFactory{
  val DB_NAME = ConfigOptions.key("db-name").stringType().noDefaultValue()
  val TABLE_NAME = ConfigOptions.key("table-name").stringType().noDefaultValue()
  val SINK_BATCH_SIZE = ConfigOptions.key("sink.batch.size") .intType().defaultValue(200)
  val SINK_BATCH_INTERVAL = ConfigOptions.key("sink.batch.interval") .durationType() .defaultValue(Duration.ofSeconds(5))
  val SINK_MAX_RETRIES = ConfigOptions.key("sink.max-retries") .intType().defaultValue(2)
  val SINK_UPDATE_MODE = ConfigOptions.key("sink.update.mode").booleanType.defaultValue(true)
}