package scala.connector.es

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.source.{DynamicTableSource, LookupTableSource, TableFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.TableFunction

import scala.collection.JavaConverters._
import scala.connector.es.EsTableFactory.esParmas
import scala.log.Logging

class EsTableSource(
  val clusterName: String,
  val resource: String,
  var physicalSchema: TableSchema
) extends LookupTableSource with Logging{

  def getLookupRuntimeProvider(context: LookupTableSource.LookupContext): LookupTableSource.LookupRuntimeProvider = {
    assert(context.getKeys.length == 1 && context.getKeys()(0).length == 1, "can only be lookup by single key")

    //必须是第一个
    assert(context.getKeys()(0)(0) == 0)

    val (nodes, user, password) = esParmas(clusterName)

    val fieldInfos = physicalSchema.getTableColumns.asScala.map { col =>
      (col.getName, col.getType)
    }

    val func = new EsRowDataLookupFunction(nodes = nodes, user = user, password = password, resource = resource, fieldInfos = fieldInfos)

    new TableFunctionProvider[RowData] {
      override def createTableFunction(): TableFunction[RowData] = func
    }
  }

  def copy(): DynamicTableSource = new EsTableSource(clusterName, resource, physicalSchema)

  def asSummaryString(): String = "myes"

}
