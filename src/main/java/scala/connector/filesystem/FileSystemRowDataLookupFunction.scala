package scala.connector.filesystem

import java.nio.charset.StandardCharsets
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.hadoop.fs.Path
import org.apache.orc
import org.apache.orc.{OrcFile, Reader, TypeDescription}

import scala.annotation.varargs
import scala.io.Source
import scala.log.Logging
import scala.util.{LoadIntervalDataUtil, SingleValueMap, Utils}
import scala.util.SingleValueMap.ResourceData
import scala.collection.JavaConverters._

class FileSystemRowDataLookupFunction(
  val path: String,
  val fieldInfos: Seq[(String, DataType)],
  val keyIndices: Array[Int],
  val deserializer: DeserializationSchema[RowData],
  val isOrc: Boolean,
  val cacheMaxSize: Int,
  val cacheExpireMs: Long
) extends TableFunction[RowData] with Logging {
  type CacheMap = util.Map[RowData, List[RowData]]
  @transient var cache: ResourceData[LoadIntervalDataUtil[CacheMap]] = null
  @transient var keyFieldGetters: Array[RowData.FieldGetter] = null
  @transient var lookupKey: GenericRowData = null
  @transient lazy val fs = new Path(path).getFileSystem(new org.apache.hadoop.conf.Configuration())

  override def open(context: FunctionContext): Unit = {
    keyFieldGetters = keyIndices.map(i => RowData.createFieldGetter(fieldInfos(i)._2.getLogicalType, i))
    cache = SingleValueMap.acquireResourceData(path, LoadIntervalDataUtil(intervalMs = cacheExpireMs) {
      if(isOrc) fetchDatasFromOrcFile() else fetchDatasFromTextFile()
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

  def getFsPaths: Seq[Path] = {
    val fsPath = new Path(path)

    if(!fs.exists(fsPath)){
      throw new Exception("文件不存在：" + path)
    }

    if(fs.isFile(fsPath)){
      List(fsPath)
    }else{
      val files = fs.listFiles(fsPath, false).toIter.map(_.getPath).filterNot(x => x.getName.startsWith("_") || x.getName.startsWith(".")).toSeq
      logWarning(s"输入文件个数：${files.length}")
      files
    }
  }


  def fetchDatasFromTextFile(): CacheMap = {
    val cacheMap = new util.HashMap[RowData, List[RowData]]()

    val fsPaths = getFsPaths
    var count = 0

    for (fsPath <- fsPaths) {
      Utils.tryWithResource(fs.open(fsPath)){ inputStream =>
        for (line <- Source.fromInputStream(inputStream, "utf-8").getLines().filter(_.trim != "")) {
          val lineByte = line.getBytes(StandardCharsets.UTF_8)
          val row = deserializer.deserialize(lineByte)
          putRowToCache(row, cacheMap)
          count += 1
          if(count > cacheMaxSize){
            throw new Exception(s"count:$count is greater than cacheMaxSize:$cacheMaxSize")
          }
        }
      }
    }

    logWarning(s"load $count rows from $path")

    cacheMap
  }

  def putRowToCache(row: RowData, cacheMap: CacheMap): Unit ={
    val key = extractKey(row)
    val rows = cacheMap.get(key)
    if(rows == null){
      cacheMap.put(key, List(row))
    }else{
      cacheMap.put(key, row::rows)
    }
  }

  def fetchDatasFromOrcFile(): CacheMap = {
    val cacheMap = new util.HashMap[RowData, List[RowData]]()

    val capacity = 2048
    val fsPaths = getFsPaths

    val getters = fieldInfos.zipWithIndex.map { case ((_, dataType), i) =>
      val func: (OrcColumnVector, Int) => Any = dataType.getLogicalType.getTypeRoot match {
        case VARCHAR | CHAR => (col, i) => col.getString(i)
        case INTEGER => (col, i) => col.getInt(i)
        case BIGINT => (col, i) => col.getLong(i)
        case FLOAT => (col, i) => col.getFloat(i)
        case DOUBLE => (col, i) => col.getDouble(i)
        case _ => throw new UnsupportedOperationException(s"unsupported data type ${dataType.getLogicalType.getTypeRoot}")
      }
      func
    }.toArray

    var count = 0

    for (fsPath <- fsPaths) {
      val conf = new org.apache.hadoop.conf.Configuration
      var reader: Reader = null
      var recordReader: orc.RecordReader = null

      try {
        reader = OrcFile.createReader(fsPath, OrcFile.readerOptions(conf))

        val requestedIds = requestedColumnIds(fieldInfos, reader).get
        val options = reader.options().include(parseInclude(reader.getSchema, requestedIds.mkString(",")))
        recordReader = reader.rows(options)

        val batch = reader.getSchema.createRowBatch(capacity)

        val orcVectorWrappers = requestedIds.zipWithIndex.map { case (idx, i) =>
          //val typ = schema.getChildren.get(idx)
          val col = batch.cols(idx)
          new OrcColumnVector(fieldInfos(i)._2, col)
        }.toArray

        while (recordReader.nextBatch(batch)) {
          var i = 0
          while (i < batch.size) {
            var j = 0

            val row = new GenericRowData(getters.length)
            while (j < getters.length) {
              val vector = orcVectorWrappers(j)
              if (!vector.isNullAt(i)) {
                row.setField(j, getters(j)(vector, i))
              }
              j += 1
            }

            putRowToCache(row, cacheMap)
            count += 1
            if (count > cacheMaxSize) {
              throw new Exception(s"count:$count is greater than cacheMaxSize:$cacheMaxSize")
            }

            i += 1
          }
        }
      } finally{
        recordReader.close()
        reader.close()
      }

    }

    logWarning(s"load $count rows from $path")

    cacheMap
  }

  def requestedColumnIds(fieldInfos: Seq[(String, DataType)], reader: Reader): Option[Array[Int]] = {
    val orcFieldNames = reader.getSchema.getFieldNames.asScala
    if (orcFieldNames.isEmpty) {
      // SPARK-8501: Some old empty ORC files always have an empty schema stored in their footer.
      None
    } else {
      if (orcFieldNames.forall(_.startsWith("_col"))) {
        // This is a ORC file written by Hive, no field names in the physical schema, assume the
        // physical schema maps to the data scheme by index.
        assert(orcFieldNames.length == fieldInfos.length, "The given data schema " +
          s"${fieldInfos.mkString(", ")} has less fields than the actual ORC physical schema, " +
          "no idea which columns were dropped, fail to read.")
        // for ORC file written by Hive, no field names
        // in the physical schema, there is a need to send the
        // entire dataSchema instead of required schema.
        // So pruneCols is not done in this case
        Some( (0 until orcFieldNames.length).toArray )
      }else{
        assert(orcFieldNames.map(_.toLowerCase).toSet.size == orcFieldNames.length, "不区分大小写后有重复的列名")

        Some(fieldInfos.map(_._1).map { name =>
          val idx = orcFieldNames.indexWhere(_.toLowerCase == name.toLowerCase)
          assert(idx >= 0, "不存在的列：" + name)
          idx
        }.toArray)
      }
    }
  }

  /**
   * [org.apache.orc.mapred.OrcInputFormat#buildOptions]
   * [org.apache.orc.mapred.OrcInputFormat#parseInclude]
   *
   * Convert a string with a comma separated list of column ids into the
   * array of boolean that match the schemas.
   *
   * @param schema     the schema for the reader
   * @param columnsStr the comma separated list of column ids
   * @return a boolean array
   */
  def parseInclude(schema: TypeDescription, columnsStr: String): Array[Boolean] = {
    if (columnsStr == null || (schema.getCategory ne TypeDescription.Category.STRUCT)){
      return null
    }

    val result = new Array[Boolean](schema.getMaximumId + 1)
    result(0) = true

    if (StringUtils.isBlank(columnsStr)){
      return result
    }

    val types = schema.getChildren
    for (idString <- columnsStr.split(",")) {
      val typeDesc = types.get(idString.trim.toInt)
      for (c <- typeDesc.getId to typeDesc.getMaximumId) {
        result(c) = true
      }
    }

    result
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
