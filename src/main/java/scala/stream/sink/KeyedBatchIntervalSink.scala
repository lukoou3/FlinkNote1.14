package scala.stream.sink

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.util.concurrent.ExecutorThreadFactory

import scala.collection.mutable.LinkedHashMap

abstract class KeyedBatchIntervalSink[T, K](
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 100L,
  replaceValue: (T, T) => T = (newValue:T, oldValue:T) => newValue
) extends RichSinkFunction[T] with CheckpointedFunction {
  @transient private var closed = false
  @transient private var scheduler: ScheduledExecutorService = _
  @transient private var scheduledFuture: ScheduledFuture[_] = _
  @transient private var lock: ReentrantLock = _
  @transient private var batch: LinkedHashMap[K, T] = _
  @transient private var flushException: Exception = _
  @transient private var lastFlushTs = 0L

  def getKey(data: T): K

  def onInit(parameters: Configuration): Unit = {}

  def onFlush(datas: collection.Map[K, T]): Unit

  def onClose(): Unit = {}

  override final def open(parameters: Configuration): Unit = {
    onInit(parameters)
    lock = new ReentrantLock()
    batch = new LinkedHashMap[K, T]
    lastFlushTs = 0L
    if (batchIntervalMs != 0 && batchSize != 1) {
      this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("BatchIntervalSink"))
      this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            if (System.currentTimeMillis() - lastFlushTs < minPauseBetweenFlushMs) {
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

  final def checkFlushException(): Unit = {
    if (flushException != null) throw new RuntimeException("flush failed.", flushException)
  }

  override final def invoke(value: T, context: SinkFunction.Context): Unit = {
    checkFlushException()
    lock.lock()
    try {
      val key = getKey(value)
      batch.get(key) match {
        case Some(oldValue) => batch += key -> replaceValue(value, oldValue)
        case None => batch += key -> value
      }

      if (batchSize > 0 && batch.size >= batchSize) {
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
      if (batch.nonEmpty) {
        onFlush(batch)
      }
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

      lock.lock()
      try {
        if (batch.size > 0) {
          flush()
        }
      } catch {
        // 这里必须的, 防止走不到onClose
        case e: Exception =>
          flushException = e
      } finally {
        lock.unlock()
      }

      onClose()
    }

    checkFlushException()
  }
}
