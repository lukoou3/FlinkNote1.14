package scala.connector.filesystem

import java.nio.charset.StandardCharsets
import java.util

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.apache.flink.table.types.DataType
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.annotation.varargs
import scala.io.Source
import scala.log.Logging
import scala.util.{LoadIntervalDataUtil, SingleValueMap}
import scala.util.SingleValueMap.ResourceData

class FileSystemRowDataLookupFunction(
  val path: String,
  val fieldInfos: Seq[(String, DataType)],
  val keyIndices: Array[Int],
  deserializer: DeserializationSchema[RowData],
  isOrc: Boolean
) extends TableFunction[RowData] with Logging {
  type CacheMap = util.Map[RowData, List[RowData]]
  @transient var cache: ResourceData[LoadIntervalDataUtil[CacheMap]] = null
  @transient var keyFieldGetters: Array[RowData.FieldGetter] = null
  @transient var lookupKey: GenericRowData = null

  override def open(context: FunctionContext): Unit = {
    keyFieldGetters = keyIndices.map(i => RowData.createFieldGetter(fieldInfos(i)._2.getLogicalType, i))
    cache = SingleValueMap.acquireResourceData(path, LoadIntervalDataUtil(intervalMs = 60000 * 10) {
      fetchDatasFromTextFile()
    })(_.stop())
    lookupKey = new GenericRowData(keyFieldGetters.length)
  }

  @varargs
  def eval(elems: AnyRef*): Unit = {
    var i = 0
    while(i < keyFieldGetters.length){
      lookupKey.setField(i, elems(i))
      i += 1
    }

    val cacheMap = cache.data.data
    val rows = cacheMap.get(lookupKey)
    if(rows != null){
      for (row <- rows) {
        collect(row)
      }
    }
  }

  def fetchDatasFromTextFile(): CacheMap = {
    val cacheMap = new util.HashMap[RowData, List[RowData]]()

    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration)
    val fsPath = new Path(path)
    if(!fs.exists(fsPath)){
      return cacheMap
    }

    val inputStream = fs.open(fsPath)

    for (line <- Source.fromInputStream(inputStream, "utf-8").getLines().filter(_.trim != "")) {
      val lineByte = line.getBytes(StandardCharsets.UTF_8)
      val row = deserializer.deserialize(lineByte)
      val key = extractKey(row)
      val rows = cacheMap.get(key)
      if(rows == null){
        cacheMap.put(key, List(row))
      }else{
        cacheMap.put(key, row::rows)
      }
    }

    inputStream.close()

    cacheMap
  }

  def fetchDatasFromOrcFile(): CacheMap = {
    val cacheMap = new util.HashMap[RowData, List[RowData]]()

    cacheMap
  }

  def extractKey(row: RowData) : RowData = {
    val key = new GenericRowData(keyFieldGetters.length)
    var i = 0
    while(i < keyFieldGetters.length){
      key.setField(i, keyFieldGetters(i).getFieldOrNull(row))
      i += 1
    }
    key
  }

  override def close(): Unit = {
    if(cache != null){
      cache.release()
    }
  }
}
