package scala.connector.filesystem

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.table.data.RowData

import scala.collection.Iterator
import scala.io.{BufferedSource, Source}

class TextFileRecordReader[T](
  path:String,
  deserializer: DeserializationSchema[T]
) extends RecordReader[T]{
  var source: BufferedSource = null
  var lines: Iterator[String] = null

  def initialize(): Unit = {
    source = Source.fromFile(path, "utf-8")
    source.getLines()
  }

  def hasNext: Boolean = lines.hasNext

  def next(): T = {
    val lineByte = lines.next().getBytes(StandardCharsets.UTF_8)
    deserializer.deserialize(lineByte)
  }

  def close(): Unit = {
    source.close()
  }
}
