package scala.connector.jdbc

import java.time.Duration
import java.util

import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, FactoryUtil}

import JdbcTableFactory._
import scala.connector.common.Utils.StringCfgOps

class JdbcTableFactory extends DynamicTableSinkFactory {

  def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    helper.validate()

    new JdbcTableSink(
      context.getCatalogTable().getResolvedSchema(),
      JdbcSinkParams(
        config.get(TABLE_NAME),
        Map.empty[String, JdbcConnectionOptions](config.get(DB_NAME)),
        config.get(SINK_BATCH_SIZE),
        config.get(SINK_BATCH_INTERVAL).toMillis,
        maxRetries = config.get(SINK_MAX_RETRIES),
        isUpdateMode = config.get(SINK_UPDATE_MODE),
        keyedMode = config.get(SINK_KEYED_MODE),
        keys = config.get(SINK_KEYED_MODE_KEYS).toSinkKeyedModeKeys,
        orderBy = config.get(SINK_KEYED_MODE_ORDERBY).toSinkKeyedModeOrderBy
      )
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
    requiredOptions.add(SINK_KEYED_MODE)
    requiredOptions.add(SINK_KEYED_MODE_KEYS)
    requiredOptions.add(SINK_KEYED_MODE_ORDERBY)
    optionalOptions
  }
}

object JdbcTableFactory {
  val DB_NAME = ConfigOptions.key("db-name").stringType().noDefaultValue()
  val TABLE_NAME = ConfigOptions.key("table-name").stringType().noDefaultValue()
  val SINK_BATCH_SIZE = ConfigOptions.key("sink.batch.size").intType().defaultValue(200)
  val SINK_BATCH_INTERVAL = ConfigOptions.key("sink.batch.interval").durationType().defaultValue(Duration.ofSeconds(5))
  val SINK_MAX_RETRIES = ConfigOptions.key("sink.max-retries").intType().defaultValue(2)
  val SINK_UPDATE_MODE = ConfigOptions.key("sink.update.mode").booleanType.defaultValue(true)
  val SINK_KEYED_MODE = ConfigOptions.key("sink.keyed.mode").booleanType.defaultValue(false)
  val SINK_KEYED_MODE_KEYS = ConfigOptions.key("sink.keyed.mode.keys").stringType().defaultValue("")
  val SINK_KEYED_MODE_ORDERBY = ConfigOptions.key("sink.keyed.mode.orderby").stringType().defaultValue("")
}