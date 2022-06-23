package scala.connector.es

import java.beans.Transient
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import scalaj.http.{Http, HttpRequest, HttpResponse}

import scala.log.Logging
import scala.util.SingleValueMap
import scala.util.SingleValueMap.ResourceData

class EsRowDataLookupFunction(
  val nodes: String,
  val user: Option[String],
  val password: Option[String],
  val resource: String,
  val fieldInfos: Seq[(String, DataType)],
  val cacheMaxSize: Int = 10000,
  val cacheExpireMs: Long = 1000 * 60 * 10
) extends TableFunction[RowData] with Logging{
  var readCntFromCache = 0L
  var readCntFromDb = 0L
  var readNullCntFromDb = 0L
  val headers = Map("content-type" -> "application/json") ++ {
    if(user.isEmpty)
      Map.empty
    else
      Map("Authorization"-> EsSecurity.basicAuthHeaderValue(user.get, password.get))
  }
  var hostIndex = 0
  @Transient var urlPrefixs: Array[String] = null
  @Transient var fieldGeters: Seq[(Int, JSONObject => Any)] = null
  @transient var cache: ResourceData[Cache[Object, RowData]] = null

  override def open(context: FunctionContext): Unit = {
    logInfo("fieldInfos:" + fieldInfos)
    assert(List(CHAR, VARCHAR).contains(fieldInfos(0)._2.getLogicalType.getTypeRoot))

    urlPrefixs = nodes.split(",").map(_.trim).filter(_.nonEmpty).map(host => s"http://$host/$resource/")

    fieldGeters = getFieldGeters

    cache = if (cacheMaxSize == -1 || cacheExpireMs == -1) {
      null
    } else {
      SingleValueMap.acquireResourceData(s"$nodes/$resource", CacheBuilder.newBuilder.
        expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
        .maximumSize(cacheMaxSize).build[Object, RowData])(_.cleanUp())
    }

    logInfo("open EsRowDataLookupFunction")
  }

  override def close(): Unit = {
    if (cache != null) {
      cache.release()
    }
    logInfo("close EsRowDataLookupFunction")
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

    val _id = key.asInstanceOf[StringData].toString
    readCntFromDb += 1

    // 连续两个host访问失败, 任务直接失败
    val result = try {
      getById(_id, hostIndex)
    } catch {
      case e: Exception =>
        logError("es_read_error", e)
        hostIndex += (hostIndex + 1) % urlPrefixs.length
        getById(_id, hostIndex)
    }

    try {
      val json = JSON.parseObject(result)
      val dataJson = json.getJSONObject("_source")

      if (dataJson != null){
        if (printLogInfo()) {
          logInfo(s"从es中获取，key：$key, key type: ${key.getClass}")
        }

        val row = new GenericRowData(fieldGeters.length + 1)
        row.setField(0, key)
        for ((i, valueGetter) <- fieldGeters) {
          row.setField(i + 1, valueGetter(dataJson))
        }

        collect(row)

        if (cache != null) {
          cache.data.put(key, row)
        }
      }else{
        readNullCntFromDb += 1
        printLogInfo()
        logInfo(s"未从es中获取到，key：$key, key type: ${key.getClass}")
      }

    } catch {
      case e: Exception =>
        logError(s"parse $result  error", e)
    }
  }

  def getById(id: String, hostIndex: Int = 0): String = {
    //val host = nodes.split(",")(hostIndex)
    //val url = s"http://$host/$resource/$id"
    val url = urlPrefixs(hostIndex) + id

    val request: HttpRequest = Http(url).headers(headers).timeout(1000 * 3, 1000 * 3)
    val response: HttpResponse[String] = request.asString
    val code: Int = response.code
    val rst: String = response.body

    // 找不到doc返回的是404, index/type不存在也是404
    // code:404, rst:{"_index":"scenic","_type":"sku","_id":"200147331080","found":false}
    if(code != 200){
      logInfo(s"code:$code, rst:$rst, url:$url")
    }
    assert(code == 200 || code == 404)

    rst
  }

  def getFieldGeters: Seq[(Int, JSONObject => Any)] = {
    fieldInfos.tail.zipWithIndex.map {case ((name, dataType), i) =>
      val func: JSONObject => Any = dataType.getLogicalType.getTypeRoot match {
        case CHAR | VARCHAR => json => BinaryStringData.fromString(json.getString(name))
        case INTEGER => json => json.getInteger(name)
        case BIGINT => json => json.getLong(name)
        case FLOAT => json => json.getFloat(name)
        case DOUBLE => json => json.getDouble(name)
        case _ => throw new UnsupportedOperationException(s"unsupported data type $dataType")
      }

      (i, func)
    }
  }

}
