package scala.file.sql

import java.nio.charset.StandardCharsets
import java.util
import java.util.List

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.IntType
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ColumnVector, LongColumnVector, TimestampColumnVector}
import org.apache.orc.TypeDescription.Category
import org.apache.orc.{OrcConf, OrcFile, TypeDescription}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.file.orc.OrcColumnVector

/**
 * orc官方api
 * https://orc.apache.org/docs/core-java.html
 */
class OrcApiSuite extends AnyFunSuite{


  /**
   * spark过滤列实现：
   * org.apache.spark.sql.execution.datasources.orc.OrcColumnarBatchReader#initialize
   * org.apache.spark.sql.execution.datasources.orc.OrcFileFormat#buildReaderWithPartitionValues
   */
  test("orc_api"){
    val conf = new org.apache.hadoop.conf.Configuration
    val reader = OrcFile.createReader(
      new Path("file:///D:/ChromeDownload/dim_common_province_a.orc"),
      OrcFile.readerOptions(conf))

    //val rows = reader.rows
    val rows = reader.rows(reader.options().include(Array(true, false, true)))
    println(rows)

    val batch = reader.getSchema.createRowBatch(20)

    println(reader.getSchema)

    while (rows.nextBatch(batch)) {
      /*for(i <- 0 until batch.size){
      }*/

      println(batch.cols.toList)
      //println(batch.toString)
    }

    rows.close()
  }


  test("orc_api2"){
    val conf = new org.apache.hadoop.conf.Configuration
    conf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, "1")
    val reader = OrcFile.createReader(
      new Path("file:///D:/ChromeDownload/orc/dim_common_province_a.orc"),
      OrcFile.readerOptions(conf))

    //val rows = reader.rows
    val options = reader.options().include(parseInclude(reader.getSchema, "1"))
    val rows = reader.rows(options)
    println(rows)

    val batch = reader.getSchema.createRowBatch(20)

    println(reader.getSchema)

    while (rows.nextBatch(batch)) {
      /*for(i <- 0 until batch.size){
      }*/

      println(batch.cols.toList)
      //println(batch.toString)
    }

    rows.close()
  }

  test("orc_api3"){
    val capacity = 1024
    // initialize
    val conf = new org.apache.hadoop.conf.Configuration
    val reader = OrcFile.createReader(
      //new Path("file:///D:/chromedownload/dim_common_province_a.orc"),
      new Path("file:///D:/chromedownload/pin_ord_stat.snappy.orc"),
      OrcFile.readerOptions(conf))

    val options = reader.options().include(parseInclude(reader.getSchema, "0,1"))
    //val recordReader = reader.rows(options)
    val recordReader = reader.rows

    val schema = reader.getSchema
    println(schema.toString)

    val getters = schema.getChildren.asScala.zip(schema.getFieldNames.asScala).zipWithIndex.map{case ((typ, name), i) =>
      val func: (ColumnVector, Int) => Any = typ.getCategory match {
        case Category.LONG => (col, i) =>
          val index = if(col.isRepeating)  0 else i
          if(col.isNull(index)){
            null
          }else{
            col.asInstanceOf[LongColumnVector].vector(index)
          }
        case Category.INT => (col, i) =>
          val index = if(col.isRepeating)  0 else i
          if(col.isNull(index)){
            null
          }else {
            col.asInstanceOf[LongColumnVector].vector(index).toInt
          }
        case Category.STRING => (col, i) =>
          val index = if(col.isRepeating)  0 else i
          if(col.isNull(index)){
            null
          }else {
            new String(
              col.asInstanceOf[BytesColumnVector].vector(index),
              col.asInstanceOf[BytesColumnVector].start(index),
              col.asInstanceOf[BytesColumnVector].length(index),
              StandardCharsets.UTF_8
            )
          }
        case Category.TIMESTAMP => (col, i) =>
          val index = if(col.isRepeating)  0 else i
          if(col.isNull(index)){
            null
          }else {
            //一个对象
            col.asInstanceOf[TimestampColumnVector].asScratchTimestamp(index)
          }
      }
      (i, name, func)
    }

    // initBatch
    val batch = reader.getSchema.createRowBatch(capacity)
    var count = 0
    while (recordReader.nextBatch(batch)) {
      batch.cols
      var i = 0
      while (i < batch.size){
        val row = getters.map{ case(j, name, getter) =>
          val value = getter(batch.cols(j), i)
          (name, value)
        }.toMap
        println(row)

        i += 1
        count += 1
      }
    }

    println(s"count:$count")

    recordReader.close()
  }

  test("orc_api4"){
    val capacity = 1024
    // initialize
    val conf = new org.apache.hadoop.conf.Configuration
    val reader = OrcFile.createReader(
      new Path("file:///D:/ChromeDownload/orc/dim_common_province_a.orc"),
      //new Path("file:///D:/chromedownload/pin_ord_stat.snappy.orc"),
      OrcFile.readerOptions(conf))

    // 过滤掉的话查到的都是null
    val options = reader.options().include(parseInclude(reader.getSchema, "0,1"))
    val recordReader = reader.rows(options)
    //val recordReader = reader.rows

    val schema = reader.getSchema
    println(schema.toString)


    // initBatch
    val batch = reader.getSchema.createRowBatch(capacity)
    val orcVectorWrappers = schema.getChildren.asScala.zip(batch.cols).map { case (typ, col) =>
      val dataType = typ.getCategory match {
        case Category.LONG => DataTypes.BIGINT()
        case Category.INT => DataTypes.INT()
        case Category.STRING => DataTypes.STRING()
        case Category.TIMESTAMP => DataTypes.BIGINT()
      }
      new OrcColumnVector(dataType, col)
    }.toArray

    val getters = schema.getChildren.asScala.zip(schema.getFieldNames.asScala).zipWithIndex.map { case ((typ, name), i) =>
      val func: (OrcColumnVector, Int) => Any = typ.getCategory match {
        case Category.LONG => (col, i) => col.getLong(i)
        case Category.INT => (col, i) => col.getInt(i)
        case Category.STRING => (col, i) => col.getString(i)
        case Category.TIMESTAMP => (col, i) => col.getLong(i)
      }
      (i, name, func)
    }.toArray

    var count = 0
    while (recordReader.nextBatch(batch)) {
      batch.cols
      var i = 0
      while (i < batch.size){
        val row = getters.map{ case(j, name, getter) =>
          val vector = orcVectorWrappers(j)
          val value = if(vector.isNullAt(i)){
            null
          } else{
            getter(vector, i)
          }
          //(name, value)
          (name, value.toString)
        }.toMap
        println(JSON.toJSONString(row.asJava, false))

        i += 1
        count += 1
      }
    }

    println(s"count:$count")

    recordReader.close()
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
      val typeDesc = types.get(idString.toInt)
      for (c <- typeDesc.getId to typeDesc.getMaximumId) {
        result(c) = true
      }
    }

    result
  }

}
