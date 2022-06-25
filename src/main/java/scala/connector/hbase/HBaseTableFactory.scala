package scala.connector.hbase

import java.time.Duration
import java.util

import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.utils.TableSchemaUtils

import HBaseTableFactory._
import scala.connector.jdbc.JdbcTableFactory.SINK_KEYED_MODE_KEYS
import scala.connector.common.Utils.StringCfgOps

class HBaseTableFactory extends DynamicTableSourceFactory with DynamicTableSinkFactory {

  def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    helper.validate()

    val physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable.getSchema)

    val fieldMap = config.get(FIELDS).split(",").flatMap { text =>
      val kv = text.trim.split(":")
      if (kv.length == 2) {
        Some((kv(0).trim, kv(1).trim))
      } else {
        None
      }
    }.toMap

    new HBaseTableSource(
      config.get(TABLE_NAME),
      config.get(CF),
      fieldMap,
      hbaseParames.getOrElse(config.get(HBASE_INSTANCE), Map.empty),
      physicalSchema,
      config.get(LOOKUP_CACHE_MAX_ROWS),
      config.get(LOOKUP_CACHE_TTL).toMillis
    )
  }

  def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    helper.validate()

    new HbaseTableSink(
      context.getCatalogTable.getResolvedSchema,
      HbaseSinkParams(
        hbaseParames.getOrElse(config.get(HBASE_INSTANCE), Map.empty),
        config.get(TABLE_NAME),
        config.get(CF),
        batchSize = config.get(SINK_BATCH_SIZE),
        batchIntervalMs = config.get(SINK_BATCH_INTERVAL).toMillis,
        keyedMode = config.get(SINK_KEYED_MODE),
        keys = config.get(SINK_KEYED_MODE_KEYS).toSinkKeyedModeKeys,
        orderBy = config.get(SINK_KEYED_MODE_ORDERBY).toSinkKeyedModeOrderBy
      )
    )
  }

  override def factoryIdentifier(): String = "myhbase"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(TABLE_NAME)
    requiredOptions.add(CF)
    requiredOptions.add(FIELDS)
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions.add(HBASE_INSTANCE)
    optionalOptions.add(LOOKUP_CACHE_MAX_ROWS)
    optionalOptions.add(LOOKUP_CACHE_TTL)
    requiredOptions.add(SINK_BATCH_SIZE)
    requiredOptions.add(SINK_BATCH_INTERVAL)
    requiredOptions.add(SINK_KEYED_MODE)
    requiredOptions.add(SINK_KEYED_MODE_KEYS)
    requiredOptions.add(SINK_KEYED_MODE_ORDERBY)
    optionalOptions
  }
}

object HBaseTableFactory {
  val TABLE_NAME = ConfigOptions.key("table-name").stringType().noDefaultValue()
  val CF = ConfigOptions.key("cf").stringType().noDefaultValue()
  val FIELDS = ConfigOptions.key("fields").stringType().noDefaultValue()
  val HBASE_INSTANCE = ConfigOptions.key("hbase-instance").stringType().defaultValue("")
  val LOOKUP_CACHE_MAX_ROWS = ConfigOptions.key("lookup.cache.max-rows").intType().defaultValue(-1)
  val LOOKUP_CACHE_TTL = ConfigOptions.key("lookup.cache.ttl").durationType().defaultValue(Duration.ofSeconds(10))
  val SINK_BATCH_SIZE = ConfigOptions.key("sink.batch.size").intType().defaultValue(200)
  val SINK_BATCH_INTERVAL = ConfigOptions.key("sink.batch.interval").durationType().defaultValue(Duration.ofSeconds(5))
  val SINK_KEYED_MODE = ConfigOptions.key("sink.keyed.mode").booleanType.defaultValue(false)
  val SINK_KEYED_MODE_KEYS = ConfigOptions.key("sink.keyed.mode.keys").stringType().defaultValue("")
  val SINK_KEYED_MODE_ORDERBY = ConfigOptions.key("sink.keyed.mode.orderby").stringType().defaultValue("")
  val hbaseParames = Map(
    "hbase-name" -> Map("kev" -> "value")
  )
}
