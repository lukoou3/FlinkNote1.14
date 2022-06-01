package scala.connector.localfile

import java.util.Optional

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.EncodingFormat
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

/**
 * 显示配置这个这个算子的并行度为1时，上游没设置并行度，得设置ChangelogMode为insert-only，或者在输出表定义唯一key
 *
 * The sink for table 'default_catalog.default_database.temp_tb' has a configured parallelism of 1, while the input parallelism is -1.
 * Since the configured parallelism is different from the input's parallelism and the changelog mode is not insert-only, a primary key is required but could not be found.
 */
class LocalFileTableSink(
  path: String,
  val encodingFormat: EncodingFormat[SerializationSchema[RowData]],
  val producedDataType: DataType,
  val insertOnly: Boolean
) extends DynamicTableSink{
  def getChangelogMode(changelogMode: ChangelogMode): ChangelogMode = {
    if(insertOnly){
      ChangelogMode.insertOnly()
    }else{
      // 全部返回
      // requestedMode
      // 不像看到before
      ChangelogMode.newBuilder
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build
    }
  }

  def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val serializer: SerializationSchema[RowData] = encodingFormat.createRuntimeEncoder(context, producedDataType)

    val func = new LocalFileSinkFunction[RowData](path, serializer)

    new SinkFunctionProvider {
      override def createSinkFunction(): SinkFunction[RowData] = func

      override def getParallelism: Optional[Integer] = Optional.of(1)
    }
  }

  def copy(): DynamicTableSink = new LocalFileTableSink(path, encodingFormat, producedDataType, insertOnly)

  def asSummaryString(): String = "LocalFileSink"
}
