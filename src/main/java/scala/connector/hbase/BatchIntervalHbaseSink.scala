package scala.connector.hbase

import java.beans.Transient
import java.util

import org.apache.commons.io.IOUtils
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}

import scala.collection.Iterable
import scala.connector.common.BatchIntervalSink
import scala.log.Logging
import scala.util.SingleValueMap

abstract class BatchIntervalHbaseSink[T](
  hbaseConf: Map[String, String],
  tableName: String,
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 100L
) extends BatchIntervalSink[T](batchSize, batchIntervalMs, minPauseBetweenFlushMs) with Logging {
  @transient var connectionResource: SingleValueMap.ResourceData[Connection] = _
  @Transient var table: Table = null
  @Transient var puts: util.List[Put] = null

  override def onInit(parameters: Configuration): Unit = {
    connectionResource = getConnectionResource()
    table = connectionResource.data.getTable(TableName.valueOf(tableName))
    puts = new util.ArrayList[Put](batchSize)
  }

  def data2EsPut(data: T): Put

  override final def onFlush(datas: Iterable[T]): Unit = {
    puts.clear()

    for (data <- datas) {
      puts.add(data2EsPut(data))
    }
    table.put(puts)

    puts.clear()
  }

  override def onClose(): Unit = {
    if(table != null){
      table.close()
    }
    if(connectionResource != null){
      connectionResource.release()
    }
  }

  def getConnectionResource(): SingleValueMap.ResourceData[Connection] = {
    SingleValueMap.acquireResourceData(hbaseConf.toSeq, {
      val config = HBaseConfiguration.create
      for ((k, v) <- hbaseConf) {
        config.set(k, v)
      }
      ConnectionFactory.createConnection(config)
    })(_.close())
  }
}
