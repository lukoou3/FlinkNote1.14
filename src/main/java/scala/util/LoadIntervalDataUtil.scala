package scala.util

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.log.Logging

/**
 * 定期更新配置的工具类，用于实现定期更新全局配置变量
 * 构造函数定义为private，只能通过apply函数创建
 * @param intervalMs        数据更新周期，第一次会立马更新
 * @param retry             更新获取数据重试次数, 1代表就执行一次(不是失败后重试1次)
 * @param retryWait         重试间隔, 0表示不延时立即重试
 * @param failOnException   重试失败后是否抛出失败异常
 * @tparam T
 */
abstract class LoadIntervalDataUtil[T] private(
  intervalMs: Long = 1000 * 60 * 10,
  retry: Int = 3,
  retryWait: Long = 1000,
  failOnException: Boolean = true
) extends Logging {
  private val started = new AtomicBoolean(false)
  private val stopped = new AtomicBoolean(false)
  private var scheduler: ScheduledExecutorService = _
  private var exception: Exception = _
  private var _data: T = _

  def loadData(): T

  def data: T = if(!failOnException || exception == null) _data else throw exception

  private def updateData(): Unit = {
    try {
      logWarning("updateData start....")
      val newData = Utils.retry("LoadIntervalDataUtil-updateData", retry, retryWait)(loadData())
      _data = newData
      logWarning("updateData end....")
    } catch {
      case e:Exception =>
        exception = e
        logError("updateDataError", e)
    }
  }

  private def start(): Unit = {
    if (started.compareAndSet(false, true)){
      updateData()
      this.scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("LoadIntervalDataUtil")
      this.scheduler.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = updateData()
      }, intervalMs,  intervalMs, TimeUnit.MILLISECONDS)
      logWarning("start....")
    }
  }

  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      if(scheduler != null){
        this.scheduler.shutdown()
      }
      logWarning("stop....")
    }
  }
}

object LoadIntervalDataUtil {

  def apply[T](
    intervalMs: Long = 1000 * 60 * 10,
    retry: Int = 3,
    retryWait: Long = 1000,
    failOnException: Boolean = true
  )(getData: => T): LoadIntervalDataUtil[T] = {
    val loadIntervalDataUtil = new LoadIntervalDataUtil[T](intervalMs, retry, retryWait, failOnException) {
      override def loadData(): T = getData
    }
    loadIntervalDataUtil.start()
    loadIntervalDataUtil
  }

}
