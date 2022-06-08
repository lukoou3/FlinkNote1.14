package scala.connector.es

import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.types.RowKind

class EsTableSink extends DynamicTableSink{

  def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build
  }

  def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = ???

  def copy(): DynamicTableSink = new EsTableSink

  def asSummaryString(): String = "EsSink"
}
