package scala.connector.hbase

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.types.RowKind

class HbaseTableSink(
  resolvedSchema: ResolvedSchema,
  params: HbaseSinkParams
) extends DynamicTableSink {

  def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build
  }

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val func = getRowDataBatchIntervalHbaseSink(resolvedSchema, params)
    SinkFunctionProvider.of(func)
  }

  override def copy(): DynamicTableSink = new HbaseTableSink( resolvedSchema, params)

  override def asSummaryString(): String = "HbaseSink"
}
