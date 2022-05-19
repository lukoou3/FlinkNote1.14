package scala.connector.socket

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType

/**
 * sink的并行度设置，可以看下这个
 * org.apache.flink.connector.print.table.PrintTableSinkFactory
 */
class SocketDynamicTableSource(
  host: String,
  port: Int,
  decodingFormat: DecodingFormat[DeserializationSchema[RowData]],
  producedDataType: DataType
) extends ScanTableSource{
  override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly

  override def getScanRuntimeProvider(runtimeProviderContext: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    val deserializer = decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType)
    val func = new SocketSourceFunction[RowData](host, port, deserializer)
    new SourceFunctionProvider() {
      override def createSourceFunction(): SourceFunction[RowData] = func
      override def isBounded: Boolean = false
    }
  }

  override def copy(): DynamicTableSource = new SocketDynamicTableSource(host, port, decodingFormat, producedDataType)

  override def asSummaryString(): String = "Socket"
}
