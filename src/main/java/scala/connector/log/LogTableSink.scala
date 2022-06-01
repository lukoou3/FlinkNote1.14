package scala.connector.log

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
 * 定义并行度。默认情况下，并行度由框架决定，和链在一起的上游 operator 一致。
 * 认为并行度是2，上游是mysocket，并行度是1，LogTableSink默认并行度是1，可以配置为2
 */
class LogTableSink(
  val logLevel: String,
  val encodingFormat: EncodingFormat[SerializationSchema[RowData]],
  val producedDataType: DataType,
  val parallelism: Optional[Integer],
  val insertOnly: Boolean // 用于校验表结果的模式
) extends DynamicTableSink{

  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    if(insertOnly){
      ChangelogMode.insertOnly
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

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    val serializer: SerializationSchema[RowData] = encodingFormat.createRuntimeEncoder(context, producedDataType)

    val func = new LogSinkFunction[RowData](logLevel, serializer)

    new SinkFunctionProvider {
      override def createSinkFunction(): SinkFunction[RowData] = func

      override def getParallelism: Optional[Integer] = parallelism
    }
  }

  override def copy(): DynamicTableSink = new LogTableSink(logLevel, encodingFormat, producedDataType, parallelism, insertOnly)

  override def asSummaryString(): String = "log-sink"
}
