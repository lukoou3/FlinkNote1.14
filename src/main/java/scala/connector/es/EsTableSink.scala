package scala.connector.es

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.types.RowKind

class EsTableSink(
  resolvedSchema: ResolvedSchema,
  params: EsSinkParams
) extends DynamicTableSink{

  def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build
  }

  def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val func = getRowDataBatchIntervalEsSink(resolvedSchema, params)
    SinkFunctionProvider.of(func, 1)
  }

  def copy(): DynamicTableSink = new EsTableSink(resolvedSchema, params)

  def asSummaryString(): String = "EsSink"
}
