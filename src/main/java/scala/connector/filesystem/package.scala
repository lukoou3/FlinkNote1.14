package scala.connector

import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator}

package object filesystem {

  implicit class RemoteIteratorIterLike(v: RemoteIterator[LocatedFileStatus]){
    def toIter: Iterator[LocatedFileStatus] = {
      new Iterator[LocatedFileStatus]() {
        override def hasNext: Boolean = v.hasNext()
        override def next(): LocatedFileStatus = v.next()
      }
    }
  }

}
