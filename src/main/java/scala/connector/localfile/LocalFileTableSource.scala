package scala.connector.localfile

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType

import scala.connector.socket.SocketSourceFunction

// LocalFileTableFactory
class LocalFileTableSource(
  filePath: String,
  sleep: Long = 10,
  decodingFormat: DecodingFormat[DeserializationSchema[RowData]],
  producedDataType: DataType
) extends ScanTableSource {
  def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly

  def getScanRuntimeProvider(runtimeProviderContext: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    val deserializer = decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType)
    val func = new LocalFileSourceFunction[RowData](filePath, sleep, deserializer)
    new SourceFunctionProvider() {
      override def createSourceFunction(): SourceFunction[RowData] = func
      override def isBounded: Boolean = false
    }
  }

  def copy(): DynamicTableSource = new LocalFileTableSource(filePath, sleep, decodingFormat, producedDataType)

  def asSummaryString(): String = "LocalFile"
}
