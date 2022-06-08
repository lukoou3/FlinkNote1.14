package scala.connector.jdbc

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.types.RowKind

class JdbcDynamicTableSink(
  val tableSchema: TableSchema,
  val dbName: String,
  val tableName: String,
  val batchSize: Int,
  val batchIntervalMs: Long,
  val maxRetries: Int,
  val isUpdateMode: Boolean
) extends DynamicTableSink{

  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build
  }

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = ???

  override def copy(): DynamicTableSink = new JdbcDynamicTableSink(tableSchema, dbName, tableName ,batchSize,batchIntervalMs, maxRetries, isUpdateMode)

  override def asSummaryString(): String = "JdbcSink"


}
