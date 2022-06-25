package scala.connector.hbase

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.types.RowKind

class HbaseTableSink(
  resolvedSchema: ResolvedSchema,
  hbaseConf: Map[String, String],
  tableName: String,
  cf: String,
  rowKey: String = "rowkey",
  qualifiers: Seq[String] = Nil,
  fieldColMap: Map[String, String] = Map.empty,
  onlyStringCol: Boolean = true,
  batchSize: Int = 500,
  batchIntervalMs: Long = 1000L,
  minPauseBetweenFlushMs: Long = 100L,
  keyedMode: Boolean = false,
  keys: Seq[String] = Nil,
  orderBy: Seq[(String, Boolean)] = Nil
) extends DynamicTableSink {

  def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build
  }

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val func = getRowDataBatchIntervalHbaseSink(resolvedSchema, hbaseConf, tableName, cf,
      rowKey=rowKey, qualifiers=qualifiers, fieldColMap=fieldColMap,onlyStringCol=onlyStringCol,
      batchSize=batchSize, batchIntervalMs=batchIntervalMs, minPauseBetweenFlushMs=minPauseBetweenFlushMs,
      keyedMode=keyedMode,keys=keys,orderBy=orderBy)
    SinkFunctionProvider.of(func)
  }

  override def copy(): DynamicTableSink = new HbaseTableSink(
    resolvedSchema,
    hbaseConf,
    tableName,
    cf,
    rowKey = rowKey,
    qualifiers = qualifiers,
    fieldColMap = fieldColMap,
    onlyStringCol = onlyStringCol,
    batchSize = batchSize,
    batchIntervalMs = batchIntervalMs,
    minPauseBetweenFlushMs = minPauseBetweenFlushMs,
    keyedMode = keyedMode,
    keys = keys,
    orderBy = orderBy
  )

  override def asSummaryString(): String = "HbaseSink"
}
