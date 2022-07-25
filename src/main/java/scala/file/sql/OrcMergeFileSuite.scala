package scala.file.sql

import com.alibaba.fastjson.JSON
import org.apache.flink.table.api.DataTypes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.OrcFile
import org.apache.orc.TypeDescription.Category
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.file.orc.OrcColumnVector

class OrcMergeFileSuite extends AnyFunSuite{
  import OrcMergeFileSuite._

  test("merge"){
    val fs = FileSystem.get(new Configuration())
    val dirPath = new Path("file:///F:/hadoop/orc_file_merge_test")
    val files = fs.listFiles(dirPath, false).toIter.map(_.getPath).filterNot(x => x.getName.startsWith("_") || x.getName.startsWith(".")).toBuffer.asJava
    files.asScala.foreach(println(_))

    val mergeingFileName = ".part_mergeing.orc"
    val mergedFileName = "part_merged.orc"

    val mergeingPath = new Path(dirPath + "/" + mergeingFileName)
    val mergedPath = new Path(dirPath + "/" + mergedFileName)
    val writerOptions = OrcFile.writerOptions(new Configuration())
    val inputPaths = files

    /*if(fs.exists(mergeingPath)){
      fs.delete(mergeingPath, false)
    }*/
    println()

    val rstPaths = OrcFile.mergeFiles(mergeingPath, writerOptions, inputPaths)
    fs.rename(mergeingPath, mergedPath)
    rstPaths.asScala.foreach(println(_))

    val crcPath = new Path(dirPath + "/" + ".part_mergeing.orc.crc")
    if(fs.exists(crcPath)){
      fs.delete(crcPath, false)
    }

    val paths = fs.listFiles(dirPath, false).toIter.map(_.getPath).filterNot(x => x.getName.startsWith("_") || x.getName.startsWith(".")).toBuffer
    readFile(paths)
  }

  test("read"){
    val fs = FileSystem.get(new Configuration())
    val fsPath = new Path("file:///F:/hadoop/orc_file_merge_test")
    val paths = fs.listFiles(fsPath, false).toIter.map(_.getPath).filterNot(x => x.getName.startsWith("_") || x.getName.startsWith(".")).toBuffer
    readFile(paths)
  }

  def readFile(paths:Seq[Path]): Unit ={
    paths.foreach{path =>
      readFile(path)
      println("\n" + "*" * 80 + "\n")
    }
  }

  def readFile(path:Path): Unit ={
    val capacity = 1024
    // initialize
    val conf = new org.apache.hadoop.conf.Configuration
    val reader = OrcFile.createReader(path,OrcFile.readerOptions(conf))

    // 过滤掉的话查到的都是null
    val options = reader.options()
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
        }.toMap.filterKeys(_ != "data")
        println(JSON.toJSONString(row.asJava, false))

        i += 1
        count += 1
      }
    }

    println(s"count:$count")

    recordReader.close()
  }

}

object OrcMergeFileSuite{
  implicit class RemoteIteratorIterLike(v:  org.apache.hadoop.fs.RemoteIterator[ org.apache.hadoop.fs.LocatedFileStatus]){
    def toIter(): Iterator[ org.apache.hadoop.fs.LocatedFileStatus] = {
      new Iterator[ org.apache.hadoop.fs.LocatedFileStatus]() {
        override def hasNext: Boolean = v.hasNext()
        override def next():  org.apache.hadoop.fs.LocatedFileStatus = v.next()
      }
    }
  }
}