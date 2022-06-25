package scala.connector.jdbc

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.types.RowKind

class JdbcTableSink(
  resolvedSchema: ResolvedSchema,
  params: JdbcSinkParams
) extends DynamicTableSink{

  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build
  }

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val func = getRowDataBatchIntervalJdbcSink(resolvedSchema, params)
    SinkFunctionProvider.of(func)
  }

  override def copy(): DynamicTableSink = new JdbcTableSink(resolvedSchema, params)

  override def asSummaryString(): String = "JdbcSink"


}
