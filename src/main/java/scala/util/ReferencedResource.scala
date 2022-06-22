package scala.util

/**
 * 一个引用计数的资源，在计数达到0之前不会释放
 * 主要用于公用的全局连接，比如hbase连接，
 */
trait ReferencedResource {
  var count: Int = 0
  def init(): Unit
  def destroy(): Unit

  def acquire() = synchronized {
    try {
      count += 1
      if (count == 1) {
        init()
      }
    } catch {
      case e: Throwable =>
        release()
        throw e
    }
  }

  def release() = synchronized {
    count -= 1
    if (count == 0) {
      destroy()
    }
  }

  def releaseOnException[T](func: => T): T = {
    acquire()
    val ret = {
      try {
        func
      } catch {
        case e: Throwable =>
          release()
          throw e
      }
    }
    ret
  }
}
