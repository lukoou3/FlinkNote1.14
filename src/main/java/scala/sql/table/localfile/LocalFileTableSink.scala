package scala.sql.table.localfile

import java.util.Optional

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.EncodingFormat
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

class LocalFileTableSink(
  path: String,
  val encodingFormat: EncodingFormat[SerializationSchema[RowData]],
  val producedDataType: DataType
) extends DynamicTableSink{
  def getChangelogMode(changelogMode: ChangelogMode): ChangelogMode = {
    // 全部返回
    // requestedMode
    // 不像看到before
    ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build
  }

  def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val serializer: SerializationSchema[RowData] = encodingFormat.createRuntimeEncoder(context, producedDataType)

    val func = new LocalFileSinkFunction[RowData](path, serializer)

    new SinkFunctionProvider {
      override def createSinkFunction(): SinkFunction[RowData] = func

      override def getParallelism: Optional[Integer] = Optional.of(1)
    }
  }

  def copy(): DynamicTableSink = new LocalFileTableSink(path, encodingFormat, producedDataType)

  def asSummaryString(): String = "LocalFileSink"
}
