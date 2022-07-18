package scala.file.sql

import java.util
import java.util.List

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcConf, OrcFile, TypeDescription}
import org.scalatest.funsuite.AnyFunSuite

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
    if (columnsStr == null || (schema.getCategory ne TypeDescription.Category.STRUCT)) return null
    val result: Array[Boolean] = new Array[Boolean](schema.getMaximumId + 1)
    result(0) = true
    if (StringUtils.isBlank(columnsStr)) return result
    val types: util.List[TypeDescription] = schema.getChildren
    for (idString <- columnsStr.split(",")) {
      val `type`: TypeDescription = types.get(idString.toInt)
      for (c <- `type`.getId to `type`.getMaximumId) {
        result(c) = true
      }
    }
    result
  }

}
