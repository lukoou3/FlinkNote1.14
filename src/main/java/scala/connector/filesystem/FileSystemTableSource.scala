package scala.connector.filesystem

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, LookupTableSource, TableFunctionProvider}
import org.apache.flink.table.data.RowData

import scala.collection.JavaConverters._
import scala.log.Logging

class FileSystemTableSource(
  path: String,
  physicalSchema: TableSchema,
  decodingFormat: DecodingFormat[DeserializationSchema[RowData]],
  isOrc: Boolean,
  cacheMaxSize: Int,
  cacheExpireMs: Long
) extends LookupTableSource with Logging{

  def getLookupRuntimeProvider(context: LookupTableSource.LookupContext): LookupTableSource.LookupRuntimeProvider = {
    val keys = context.getKeys
    assert(keys.forall(_.length == 1),  "can only be lookup by simple type key")
    val keyIndices = keys.map(_.apply(0))

    val fieldInfos = physicalSchema.getTableColumns.asScala.map { col =>
      (col.getName, col.getType)
    }

    val deserializer = if(isOrc) null else decodingFormat.createRuntimeDecoder(context, physicalSchema.toPhysicalRowDataType)

    val func = new FileSystemRowDataLookupFunction(path, fieldInfos, keyIndices, deserializer, isOrc, cacheMaxSize, cacheExpireMs)

    TableFunctionProvider.of(func)
  }

  def copy(): DynamicTableSource = new FileSystemTableSource(path, physicalSchema, decodingFormat, isOrc, cacheMaxSize, cacheExpireMs)

  def asSummaryString(): String = "myfilesystem"
}
