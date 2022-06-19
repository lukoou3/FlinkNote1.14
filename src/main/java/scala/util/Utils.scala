package scala.util

import java.io.{Closeable, InputStream}

import scala.annotation.tailrec
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

  /**
   * 执行某个函数, 最多重试n次。使用尾递归优化，不用担心栈溢出
   *
   * @param funcName  执行的逻辑名称, 用于显示日志排查错误
   * @param n         重试次数
   * @param waitSleep 重试时是否延时, 默认=0不延时立即重试
   * @param func      逻辑
   * @tparam T
   * @return
   */
  def retry[T](funcName: String, n: Int, waitSleep: Long = 0L)(func: => T): T = {
    retryTimes(func, funcName, n, waitSleep)
  }

  @tailrec
  private def retryTimes[T](func: => T, funcName: String, n: Int, waitSleep: Long, times: Int = 1): T = {
    Try(func) match {
      case Success(x) => x
      case Failure(e) if times < n =>
        logWarning(s"retry func($funcName) failed for $times times error msg: ${e.getMessage}")
        if (waitSleep > 0L) {
          Thread.sleep(waitSleep)
        }
        retryTimes(func, funcName, n, waitSleep, times + 1)
      case Failure(e) =>
        logError(s"retry func($funcName) $n times finally failed ", e)
        throw e
    }
  }

  /**
   * 执行某个函数, 支持重试, 重试时超过最大执行时长抛出异常。使用尾递归优化，不用担心栈溢出
   * @param funcName  执行的逻辑名称, 用于显示日志排查错误
   * @param duration  重试时支持的最大执行时长
   * @param waitSleep 重试时是否延时, 默认=0不延时立即重试
   * @param func      逻辑
   * @tparam T
   * @return
   */
  def retryAuration[T](funcName: String, duration: Long, waitSleep: Long = 0L)(func: => T): T = {
    retryAurationTimes(func, funcName, duration, waitSleep)
  }

  @tailrec
  private def retryAurationTimes[T](func: => T, funcName: String, duration: Long, waitSleep: Long,
    startTime: Long = System.currentTimeMillis(), times: Int = 1): T = {
    val thisStartTime = System.currentTimeMillis()
    Try(func) match {
      case Success(x) => x
      case Failure(e) =>
        val endTime = System.currentTimeMillis()
        if (endTime - startTime < duration) {
          logWarning(s"retryAuration func($funcName) failed for $times times(${endTime - thisStartTime} ms) ${endTime - startTime} ms error msg: ${e.getMessage}")
          if (waitSleep > 0L) {
            Thread.sleep(waitSleep)
          }
          retryAurationTimes(func, funcName, duration, waitSleep, startTime, times + 1)
        } else {
          logError(s"retryAuration func($funcName) $times times(${endTime - thisStartTime} ms) ${endTime - startTime} ms finally failed ", e)
          throw e
        }
    }
  }
}
