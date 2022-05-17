package scala.stream.sink

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.util.concurrent.ExecutorThreadFactory
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.mutable.ArrayBuffer
import scala.util.ThreadUtils

/**
 * 参照官方的jdbcsink写的,实现BatchIntervalSink
 * @param batchSize
 * @param batchIntervalMs
 * @param minPauseBetweenFlushMs 定时任务Flush和上次Flush最小间隔
 * @tparam T
 */
abstract class BatchIntervalSink[T](batchSize: Int, batchIntervalMs: Long, minPauseBetweenFlushMs: Long = 100L) extends RichSinkFunction[T] with CheckpointedFunction{
  @transient
  private var batchCount = 0
  @transient
  private var closed = false
  @transient
  private var scheduler: ScheduledExecutorService = _
  @transient
  private var scheduledFuture: ScheduledFuture[_] = _
  @transient
  private var lock: ReentrantLock = _
  @transient
  private var batch: ArrayBuffer[T] = _
  @transient
  private var flushException: Exception = _
  @transient
  private var lastFlushTs = 0L

  def onInit(parameters: Configuration): Unit

  def onFlush(datas: Seq[T]): Unit

  def onClose(): Unit

  def valueTransform(data: T): T = data

  override final def open(parameters: Configuration): Unit = {
    onInit(parameters)
    lock = new ReentrantLock()
    batch = new ArrayBuffer[T]
    lastFlushTs = 0L
    if (batchIntervalMs != 0 && batchSize != 1) {
      //this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("BatchIntervalSink"))
      this.scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("BatchIntervalSink")
      this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            if(System.currentTimeMillis() - lastFlushTs < minPauseBetweenFlushMs) {
              return
            }

            lock.lock()
            try {
              flush()
            } catch {
              case e: Exception =>
                flushException = e
            }
            finally {
              lock.unlock()
            }
          }
        },
        batchIntervalMs,
        batchIntervalMs,
        TimeUnit.MILLISECONDS
      )
    }
  }

  final def  checkFlushException(): Unit = {
    if (flushException != null) throw new RuntimeException("flush failed.", flushException)
  }

  override final def invoke(value: T, context: SinkFunction.Context): Unit = {
    checkFlushException()
    lock.lock()
    try {
      batch += valueTransform(value)
      batchCount += 1
      if (batchSize > 0 && batchCount >= batchSize) {
        flush()
      }
    } finally {
      lock.unlock()
    }
  }
  final def flush(): Unit = {
    checkFlushException()
    lastFlushTs = System.currentTimeMillis()
    lock.lock()
    try {
      if(batch.nonEmpty){
        onFlush(batch)
      }
      batchCount = 0
      batch.clear()
    } finally {
      lock.unlock()
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    flush()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

  }

  override final def close(): Unit = this.synchronized {
    if (!closed) {
      closed = true

      if (this.scheduledFuture != null) {
        this.scheduledFuture.cancel(false)
        this.scheduler.shutdown()
      }

      // init中可能抛出异常
      if(lock != null){
        lock.lock()
        try {
          if (batchCount > 0) {
            flush()
          }
        } catch {
          // 这里必须的, 防止走不到onClose
          case e: Exception =>
            flushException = e
        }finally {
          lock.unlock()
        }
      }

      onClose()
    }

    checkFlushException()
  }

}
