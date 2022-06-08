package scala.connector.jdbc

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.types.RowKind

class JdbcTableSink(
  val resolvedSchema: ResolvedSchema,
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

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val func = getRowDataBatchIntervalJdbcSink(resolvedSchema, tableName, null, batchSize, batchIntervalMs, maxRetries = maxRetries, isUpdateMode=isUpdateMode)
    SinkFunctionProvider.of(func)
  }

  override def copy(): DynamicTableSink = new JdbcTableSink(resolvedSchema, dbName, tableName ,batchSize,batchIntervalMs, maxRetries, isUpdateMode)

  override def asSummaryString(): String = "JdbcSink"


}