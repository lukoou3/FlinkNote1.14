package scala.connector.hbase

import java.beans.Transient
import java.util.concurrent.TimeUnit

import org.apache.flink.shaded.guava30.com.google.common.cache.{Cache, CacheBuilder}
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, CHAR, DOUBLE, FLOAT, INTEGER, VARCHAR}
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Get, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.log.Logging
import scala.util.SingleValueMap
import scala.util.SingleValueMap.ResourceData

class HBaseRowDataLookupFunction(
  val tableName: String,
  val fieldInfos: Seq[(String, String, DataType)],
  val cacheMaxSize: Int,
  val cacheExpireMs: Long
) extends TableFunction[RowData] with Logging {
  var readCntFromCache = 0L
  var readCntFromDb = 0L
  var readNullCntFromDb = 0L
  @transient var cache: ResourceData[Cache[Object, RowData]] = null
  @Transient var rowKeyFunc: AnyRef => Array[Byte] = null
  @Transient var fieldGeters: Seq[(Int, Array[Byte], Array[Byte], Array[Byte] => Any)] = null
  @Transient var table: Table = null

  override def open(context: FunctionContext): Unit = {
    rowKeyFunc = getRowKeyFunc
    fieldGeters = getFieldGeters

    cache = if (cacheMaxSize == -1 || cacheExpireMs == -1) {
      null
    } else {
      SingleValueMap.acquireResourceData(tableName, CacheBuilder.newBuilder.
        expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
        .maximumSize(cacheMaxSize).build[Object, RowData])(_.cleanUp())
    }

    //table = HBaseClient.getConnection().getTable(TableName.valueOf(tableName))

    //HBaseClient.addRuningTask()

    logInfo("open HBaseRowDataLookupFunction")
  }

  override def close(): Unit = {
    if (table != null) {
      try {
        table.close()
      } catch {
        case e: Exception =>
          logError("hbase table close error", e)
      }
    }
    //HBaseClient.colseConn()
    if (cache != null) {
      SingleValueMap.release(cache)
    }
    logInfo("close HBaseRowDataLookupFunction")
  }

  def printLogInfo(): Boolean = {
    val cnt = readCntFromCache + readCntFromDb
    val flag = (cnt <= 100 && cnt % 10 == 0) || (cnt > 100 && cnt <= 1000 && cnt % 100 == 0) ||
      (cnt > 1000 && cnt <= 10000 && cnt % 1000 == 0) || (cnt > 10000 && cnt % 10000 == 0)
    if (flag) {
      logInfo(s"readCntFromCache:$readCntFromCache,readCntFromDb:$readCntFromDb,readNullCntFromDb:$readNullCntFromDb")
    }
    flag
  }

  def eval(key: AnyRef): Unit = {
    if (cache != null) {
      val cacheRowData = cache.data.getIfPresent(key)
      if (cacheRowData != null) {
        readCntFromCache += 1
        if (printLogInfo()) {
          logInfo(s"从cache中获取，key：$key, key type: ${key.getClass}")
        }

        collect(cacheRowData)
        return
      }
    }

    val rowkey = rowKeyFunc(key)

    val get = new Get(rowkey)
    val result = table.get(get)

    readCntFromDb += 1
    if (result != null && !result.isEmpty) {
      if (printLogInfo()) {
        logInfo(s"从hbase中获取，key：$key, key type: ${key.getClass}")
      }

      val row = new GenericRowData(fieldGeters.length + 1)
      row.setField(0, key)
      fieldGeters.foreach { case (i, cf, col, valueGetter) =>
        val kv = result.getColumnLatestCell(cf, col)
        val value = if (kv == null || kv.getValueLength == 0) {
          null
        } else {
          val v = CellUtil.cloneValue(kv)
          valueGetter(v)
        }
        row.setField(i + 1, value)
      }

      collect(row)

      if (cache != null) {
        cache.data.put(key, row)
      }
    } else {
      readNullCntFromDb += 1
      printLogInfo()
      logInfo(s"未从hbase中获取到，key：$key, key type: ${key.getClass}")
    }

  }

  def getRowKeyFunc = {
    val (_, _, dataType) = fieldInfos(0)
    val func: AnyRef => Array[Byte] = dataType.getLogicalType.getTypeRoot match {
      case CHAR | VARCHAR => x => x.asInstanceOf[StringData].toBytes
      case INTEGER => x => Bytes.toBytes(x.asInstanceOf[Int])
      case BIGINT => x => Bytes.toBytes(x.asInstanceOf[Long])
      case FLOAT => x => Bytes.toBytes(x.asInstanceOf[Float])
      case DOUBLE => x => Bytes.toBytes(x.asInstanceOf[Double])
      case _ => throw new UnsupportedOperationException(s"unsupported data type $dataType")
    }
    func
  }

  def getFieldGeters: Seq[(Int, Array[Byte], Array[Byte], Array[Byte] => Any)] = {
    fieldInfos.tail.zipWithIndex.map { case ((cf, col, dataType), i) =>
      val func: Array[Byte] => Any = dataType.getLogicalType.getTypeRoot match {
        case CHAR | VARCHAR => src => BinaryStringData.fromBytes(src)
        case INTEGER => src => Bytes.toInt(src)
        case BIGINT => src => Bytes.toLong(src)
        case FLOAT => src => Bytes.toFloat(src)
        case DOUBLE => src => Bytes.toDouble(src)
        case _ => throw new UnsupportedOperationException(s"unsupported data type $dataType")
      }

      (i, Bytes.toBytes(cf), Bytes.toBytes(col), func)
    }
  }
}
