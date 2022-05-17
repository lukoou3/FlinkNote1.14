package scala.util

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

}
