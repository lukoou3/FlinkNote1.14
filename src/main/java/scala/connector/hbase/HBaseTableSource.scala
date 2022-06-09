package scala.connector.hbase

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.source.{DynamicTableSource, LookupTableSource, TableFunctionProvider}

import scala.collection.JavaConverters._
import scala.log.Logging

class HBaseTableSource(
  val tableName: String,
  val cf: String,
  val fieldMap: Map[String, String],
  val parames: Map[String, String],
  var physicalSchema: TableSchema,
  val cacheMaxSize: Int,
  val cacheExpireMs: Long
) extends LookupTableSource with Logging{
  def getLookupRuntimeProvider(context: LookupTableSource.LookupContext): LookupTableSource.LookupRuntimeProvider = {
    assert(context.getKeys.length == 1 && context.getKeys()(0).length == 1, "can only be lookup by single key")

    //必须是第一个
    assert(context.getKeys()(0)(0) == 0)

    val fieldInfos = physicalSchema.getTableColumns.asScala.map { col =>
      (cf, fieldMap.getOrElse(col.getName, col.getName), col.getType)
    }

    val func = new HBaseRowDataLookupFunction(tableName, fieldInfos, cacheMaxSize, cacheExpireMs)

    logInfo(s"HBaseRowDataLookupFunction:tableName=$tableName,fieldInfos=$fieldInfos,cacheMaxSize=$cacheMaxSize,cacheExpireMs=$cacheExpireMs")

    TableFunctionProvider.of(func)
  }

  def copy(): DynamicTableSource = ???

  def asSummaryString(): String = "myhbase"
}
