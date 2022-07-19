package scala.connector.filesystem

import java.util

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, ReadableConfig}
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.utils.TableSchemaUtils

import FileSystemTableFactory._
import scala.collection.JavaConverters._

class FileSystemTableFactory extends DynamicTableSourceFactory{
  def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    validateRequiredOptions(config)

    val decodingFormat: DecodingFormat[DeserializationSchema[RowData]] = helper.discoverDecodingFormat(
      classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)

    val physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable.getSchema)

    new FileSystemTableSource(
      config.get(PATH),
      physicalSchema,
      decodingFormat,
      false
    )
  }

  def factoryIdentifier(): String = "myfilesystem"

  def validateRequiredOptions(config: ReadableConfig): Unit = {
    for (option <- requiredOptions().asScala) {
      assert(config.get(option) != null, option.key() + "参数必须设置")
    }
  }

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(PATH)
    requiredOptions.add(FactoryUtil.FORMAT)
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions
  }
}

object FileSystemTableFactory{
  val PATH = ConfigOptions.key("path").stringType.noDefaultValue

}
