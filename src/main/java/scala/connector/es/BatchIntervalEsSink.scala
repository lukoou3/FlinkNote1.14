package scala.connector.es

import org.apache.flink.configuration.Configuration

import scala.collection.Iterable
import scala.connector.common.BatchIntervalSink
import scala.log.Logging

abstract class BatchIntervalEsSink[T, U](
  cfg: Map[String, String],
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 100L
)extends BatchIntervalSink[T](batchSize, batchIntervalMs, minPauseBetweenFlushMs) with Logging {
  @transient var esWriter: EsWriter[U] = _

  override def onInit(parameters: Configuration): Unit = {
    esWriter = new EsWriter[U](cfg)
    esWriter.init()
  }

  def data2EsRecord(data: T): U

  override final def onFlush(datas: Iterable[T]): Unit = {
    for (data <- datas) {
      esWriter.write(data2EsRecord(data))
    }
    esWriter.flush()
  }

  override def onClose(): Unit = {
    if(esWriter != null){
      esWriter.close()
    }
  }
}
