package scala.connector.filesystem

import java.io.Closeable

trait RecordReader[T] extends Closeable with Serializable {

  def initialize(): Unit

  def hasNext: Boolean

  def next(): T

}
