package scala.stream.func

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{StateInitializationContext, StateSnapshotContext}
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, ChainingStrategy, OneInputStreamOperator, TimestampedCollector}
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.log.Logging

/**
 *
 * @param batchSize               batchSize
 * @param batchIntervalMs         batchIntervalMs
 * @param minPauseBetweenFlushMs  定时器Flush和上次Flush最小间隔
 * @param useState                使用状态(默认为true), 会校验必须开启cp, 不使用状态会丢数据
 * @param isChainHead             ChainingStrategy.HEAD(默认为true)
 * @param flatMapper              处理元素输出到下游, 输入的batch seq是同一个对象, 下游不能缓存batch seq
 * @tparam T
 * @tparam O
 */
class BatchIntervalFlatMapOperator[T: TypeInformation, O] private(
  val batchSize: Int,
  val batchIntervalMs: Long,
  val minPauseBetweenFlushMs: Long,
  val useState: Boolean,
  val isChainHead: Boolean,
  val flatMapper: FlatMapFunction[Seq[T], O]
) extends AbstractStreamOperator[O] with OneInputStreamOperator[T, O] with ProcessingTimeCallback with Logging {
  val batch = new ArrayBuffer[T]
  var hasTimestamp = false
  @transient var lastFlushTs = 0L
  @transient var listState: ListState[T] = _
  @transient var collector: TimestampedCollector[O] = _
  if(!isChainHead){
    setChainingStrategy(ChainingStrategy.ALWAYS)
  }else{
    setChainingStrategy(ChainingStrategy.HEAD)
  }

  override def open(): Unit = {
    super.open()
    val isCheckpointingEnabled = getRuntimeContext.isCheckpointingEnabled
    if(useState && !isCheckpointingEnabled){
      throw new Exception("useState must enable checkpoint")
    }

    logWarning(s"BatchIntervalOperator isCheckpointingEnabled:$isCheckpointingEnabled")
    logWarning("BatchIntervalOperator open...")

    collector = new TimestampedCollector(output)
    if(batch.nonEmpty){
      logWarning(s"open batch: ${batch.size}: ${batch}")
    }
    val currentTime = getProcessingTimeService().getCurrentProcessingTime
    getProcessingTimeService().registerTimer(currentTime + batchIntervalMs, this)
  }

  override def initializeState(context: StateInitializationContext): Unit = {
    logWarning("BatchIntervalOperator initializeState...")
    if(useState){
      val stateDescriptor = new ListStateDescriptor[T]("batch-interval-op", createTypeInformation[T])
      listState = context.getOperatorStateStore().getListState(stateDescriptor);
      if (context.isRestored()) {
        listState.get().asScala.foreach(x => batch += x)
        logWarning(s"initializeState: ${batch.size}: ${batch}")
      }
    }
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    logWarning("BatchIntervalOperator snapshotState...")
    if(useState){
      listState.clear();
      if (batch.size > 0) {
        logWarning(s"snapshotState: ${batch.size}: ${batch}")
        listState.addAll(batch.asJava)
      }
    }
  }

  override def processElement(element: StreamRecord[T]): Unit = {
    collector.setTimestamp(element)
    hasTimestamp = true
    batch += element.getValue
    if(batch.size >= batchSize){
      flush()
    }
  }

  final def flush(): Unit = synchronized {
    lastFlushTs = System.currentTimeMillis()
    flatMapper.flatMap(batch, collector)
    batch.clear()
  }


  override def close(): Unit = {
    logWarning("BatchIntervalOperator close...")
    //会先调用sink的close
    /*if(batch.nonEmpty){
      flush()
    }*/
  }


  override def finish(): Unit = {
    super.finish()
    logWarning("BatchIntervalOperator finish...")
  }

  override def onProcessingTime(timestamp: Long): Unit = {
    if(batch.nonEmpty && System.currentTimeMillis() - lastFlushTs >= minPauseBetweenFlushMs){
      flush()
    }
    val currentTime = getProcessingTimeService().getCurrentProcessingTime
    getProcessingTimeService().registerTimer(currentTime + batchIntervalMs, this)
  }

}

object BatchIntervalFlatMapOperator {

  implicit class DataStreamBatchIntervalFlatMapOps[T](ds: DataStream[T]) {

    def batchIntervalFlatMap[O](
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      useState: Boolean = true,
      isChainHead: Boolean = true
    )(flatMapper: FlatMapFunction[Seq[T], O])(implicit dataType: TypeInformation[T], outType: TypeInformation[O]): DataStream[O] = {
      val operator = new BatchIntervalFlatMapOperator[T, O](batchSize, batchIntervalMs, minPauseBetweenFlushMs, useState, isChainHead, flatMapper)
      ds.transform("batch-interval-flatmap", operator)
    }

    def batchIntervalFlatMap2[O](
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      useState: Boolean = true,
      isChainHead: Boolean = true
    )(func: (Seq[T], Collector[O]) => Unit)(implicit dataType: TypeInformation[T], outType: TypeInformation[O]): DataStream[O] = {
      val flatMapper = new FlatMapFunction[Seq[T], O] {
        def flatMap(in: Seq[T], out: Collector[O]){ func(in, out) }
      }
      val operator = new BatchIntervalFlatMapOperator[T, O](batchSize, batchIntervalMs, minPauseBetweenFlushMs, useState, isChainHead, flatMapper)
      ds.transform("batch-interval-flatmap", operator)
    }

    def batchIntervalFlatMap3[O](
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      useState: Boolean = true,
      isChainHead: Boolean = true
    )(func: Seq[T] => TraversableOnce[O])(implicit dataType: TypeInformation[T], outType: TypeInformation[O]): DataStream[O] = {
      val flatMapper = new FlatMapFunction[Seq[T], O] {
        def flatMap(in: Seq[T], out: Collector[O]) { func(in) foreach out.collect }
      }
      val operator = new BatchIntervalFlatMapOperator[T, O](batchSize, batchIntervalMs, minPauseBetweenFlushMs, useState, isChainHead, flatMapper)
      ds.transform("batch-interval-flatmap", operator)
    }

    def batchIntervalMap[O](
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      useState: Boolean = true,
      isChainHead: Boolean = true
    )(mapper: MapFunction[Seq[T], O])(implicit dataType: TypeInformation[T], outType: TypeInformation[O]): DataStream[O] = {
      val flatMapper = new FlatMapFunction[Seq[T], O] {
        def flatMap(in: Seq[T], out: Collector[O]){ out.collect(mapper.map(in)) }
      }
      val operator = new BatchIntervalFlatMapOperator[T, O](batchSize, batchIntervalMs, minPauseBetweenFlushMs, useState, isChainHead, flatMapper)
      ds.transform("batch-interval-map", operator)
    }

    def batchIntervalMap2[O](
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      useState: Boolean = true,
      isChainHead: Boolean = true
    )(func: Seq[T] => O)(implicit dataType: TypeInformation[T], outType: TypeInformation[O]): DataStream[O] = {
      val flatMapper = new FlatMapFunction[Seq[T], O] {
        def flatMap(in: Seq[T], out: Collector[O]){ out.collect(func(in)) }
      }
      val operator = new BatchIntervalFlatMapOperator[T, O](batchSize, batchIntervalMs, minPauseBetweenFlushMs, useState, isChainHead, flatMapper)
      ds.transform("batch-interval-map", operator)
    }

  }
}
