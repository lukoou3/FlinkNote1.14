package scala.connector.es

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.types.RowKind
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

import scala.connector.es.EsTableFactory.esParmas

class EsTableSink(
  resolvedSchema: ResolvedSchema,
  clusterName: String,
  resource: String,
  cfg: Map[String, String],
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 200L,
  keyedMode: Boolean = false,
  keys: Seq[String] = Nil,
  orderBy: Seq[(String, Boolean)] = Nil,
  updateScriptOrderBy: Seq[(String, Boolean)] = Nil
) extends DynamicTableSink{

  def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build
  }

  def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val (nodes, user, password) = esParmas(clusterName)
    val rstCfg = Map[String, String](
      ES_NODES -> nodes,
      ES_RESOURCE_WRITE -> resource,
      ES_MAPPING_EXCLUDE -> "_id"
    ) ++ {
      if(user.nonEmpty) Map(ES_NET_HTTP_AUTH_USER -> user.get, ES_NET_HTTP_AUTH_PASS -> password.get) else Map.empty
    } ++ cfg
    val func = getRowDataBatchIntervalEsSink(resolvedSchema, rstCfg, batchSize, batchIntervalMs, minPauseBetweenFlushMs,
      keyedMode=keyedMode,keys=keys,orderBy=orderBy, updateScriptOrderBy=updateScriptOrderBy
    )
    SinkFunctionProvider.of(func, 1)
  }

  def copy(): DynamicTableSink = new EsTableSink(
    resolvedSchema,
    clusterName,
    resource,
    cfg,
    batchSize,
    batchIntervalMs,
    minPauseBetweenFlushMs,
    keyedMode,
    keys,
    orderBy,
    updateScriptOrderBy
  )

  def asSummaryString(): String = "EsSink"
}
