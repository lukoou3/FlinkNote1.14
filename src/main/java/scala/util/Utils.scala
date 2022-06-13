package scala.util

import java.io.{Closeable, InputStream}

import scala.collection.mutable
import scala.io.Source
import scala.log.Logging
import scala.util.control.ControlThrowable

object Utils extends Logging{

  /**
   * 执行给定的块，记录并重新抛出任何未捕获的异常。
   * 这对于包装在线程中运行的代码、确保打印异常以及避免捕获可丢弃的代码特别有用。
   * Execute the given block, logging and re-throwing any uncaught exception.
   * This is particularly useful for wrapping code that runs in a thread, to ensure
   * that exceptions are printed, and to avoid having to catch Throwable.
   */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      // scala中实现控制的一些异常，这个不需要打印日志
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  def getYAMLResourceConf(is: InputStream, enc: String = "utf-8"): Map[String, String] ={
    val map = new mutable.HashMap[String, String]()
    for ((line, lineNo) <- Source.fromInputStream(is, enc).getLines().zipWithIndex) {
      // 1. check for comments
      val comments = line.split("#", 2)
      val conf = comments(0).trim

      // 2. get key and value
      if (conf.length() > 0) {
        val kv = conf.split(": ", 2).map(_.trim)

        // skip line with no valid key-value pair
        if (kv.length == 1 || kv.exists(_.isEmpty)) {
          logWarning(s"error key and value in lineNo:$lineNo configuration:$conf")
        }else{
          map.put(kv(0), kv(1))
        }

      }
    }
    map.toMap
  }
}
