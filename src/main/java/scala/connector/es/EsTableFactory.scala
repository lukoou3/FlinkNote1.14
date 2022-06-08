package scala.connector.es

import java.util

import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.utils.TableSchemaUtils

import EsTableFactory._

class EsTableFactory extends DynamicTableSourceFactory with DynamicTableSinkFactory{
  def factoryIdentifier(): String = "myes"

  def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    helper.validate()

    val physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable.getSchema)

    new EsTableSource(config.get(CLUSTER_NAME), config.get(RESOURCE), physicalSchema)
  }

  def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    helper.validate()


  }

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(CLUSTER_NAME)
    requiredOptions.add(RESOURCE)
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions
  }
}
object EsTableFactory{
  val CLUSTER_NAME = ConfigOptions.key("cluster-name").stringType().noDefaultValue()
  val RESOURCE = ConfigOptions.key("resource").stringType().noDefaultValue()

  val esParmas = Map[String, (String, Option[String], Option[String])](
    "cluster-name" -> ("cluster-name", None, None)
  )
}

