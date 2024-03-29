package scala.connector.common

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.Iterable
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.log.Logging
import scala.util.ThreadUtils

/**
 * 参照官方的jdbcsink写的,实现BatchIntervalSink
 * @param batchSize
 * @param batchIntervalMs
 * @param minPauseBetweenFlushMs 定时任务Flush和上次Flush最小间隔
 * @tparam T
 */
abstract class BatchIntervalSink[T](
  val batchSize: Int,
  val batchIntervalMs: Long,
  val minPauseBetweenFlushMs: Long = 100L,
  val keyedMode: Boolean = false
) extends RichSinkFunction[T] with CheckpointedFunction with Logging{
  @transient private var closed = false
  @transient private var scheduler: ScheduledExecutorService = _
  @transient private var scheduledFuture: ScheduledFuture[_] = _
  @transient private var lock: ReentrantLock = _
  @transient private var batch: ArrayBuffer[T] = _
  @transient private var keyedBatch: LinkedHashMap[Any, T] = _
  @transient private var flushException: Exception = _
  @transient private var lastFlushTs = 0L
  @transient private var writeCount = 0L
  @transient private var writeBytes = 0L
  @transient private var numBytesOutCounter: Counter = _
  @transient private var numRecordsOutCounter: Counter = _

  def onInit(parameters: Configuration): Unit

  def onFlush(datas: Iterable[T]): Unit

  def onClose(): Unit

  def valueTransform(data: T): T = data

  def getKey(data: T): Any = throw new Exception("keyedMode必须实现")

  def replaceValue(newValue:T, oldValue:T): T = newValue

  override def logName = classOf[BatchIntervalSink[_]].getName

  override final def open(parameters: Configuration): Unit = {
    onInit(parameters)
    numBytesOutCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup.getNumBytesOutCounter
    numRecordsOutCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup.getNumRecordsOutCounter

    lock = new ReentrantLock()
    if(!keyedMode){
      batch = new ArrayBuffer[T]
    }else{
      keyedBatch = new LinkedHashMap[Any, T]
    }
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

  final def checkFlushException(): Unit = {
    if (flushException != null) throw new RuntimeException("flush failed.", flushException)
  }
  final def currentBatchCount: Int = if(!keyedMode) batch.size else keyedBatch.size

  override final def invoke(value: T, context: SinkFunction.Context): Unit = {
    checkFlushException()
    lock.lock()
    try {
      if(!keyedMode){
        batch += valueTransform(value)
      }else{
        val newValue = valueTransform(value)
        val key = getKey(newValue)
        keyedBatch.get(key) match {
          case Some(oldValue) => keyedBatch += key -> replaceValue(newValue, oldValue)
          case None => keyedBatch += key -> newValue
        }
      }

      if (batchSize > 0 && currentBatchCount >= batchSize) {
        flush()
      }
    } finally {
      lock.unlock()
    }
  }

  final def incNumBytesOut(bytes: Long): Unit = {
    writeBytes += bytes
    numBytesOutCounter.inc(bytes)
  }

  final def flush(): Unit = {
    checkFlushException()
    lastFlushTs = System.currentTimeMillis()
    if(currentBatchCount <= 0){
      return
    }
    lock.lock()
    try {
      if(!keyedMode){
        onFlush(batch)
        writeCount += currentBatchCount
        numRecordsOutCounter.inc(currentBatchCount)
        batch.clear()
      }else{
        onFlush(keyedBatch.values)
        writeCount += currentBatchCount
        numRecordsOutCounter.inc(currentBatchCount)
        keyedBatch.clear()
      }

      logWarning(s"writeCount:$writeCount,writeBytes:$writeBytes")
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
          if (currentBatchCount > 0) {
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

