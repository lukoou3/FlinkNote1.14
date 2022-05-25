package scala.stream.join

import java.lang

import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 自定义函数实现二流join, scala版本，需引入隐式转换
 *
 * @param ttlConfs              ValueState的ttl失效时间
 * @param delayMs            流中元素等待的最长时间,超时就输出
 * @param isReplaceAndHandleData1 (newData, oldData) => (是否比上一个数据新, 是否处理数据)
 * @param isReplaceAndHandleData2 (newData, oldData) => (是否比上一个数据新, 是否处理数据)
 * @tparam K  key
 * @tparam D1 data1
 * @tparam D2 data2
 */
class Unoin2JoinKeyedProcessFunction[K, D1: TypeInformation, D2: TypeInformation](
  ttlConfs: (Option[StateTtlConfig], Option[StateTtlConfig]) = (TtlConf(), TtlConf()),
  delayMs: Long = 3000,
  isReplaceAndHandleData1: (D1, D1) => (Boolean, Boolean) = (newData:D1, oldData:D1) => (true, true),
  isReplaceAndHandleData2: (D2, D2) => (Boolean, Boolean) = (newData:D2, oldData:D2) => (true, true)
)
  extends KeyedProcessFunction[K, (K, Option[D1], Option[D2]), (K, Option[D1], Option[D2])] {
  type I = (K, Option[D1], Option[D2])
  type O = (K, Option[D1], Option[D2])
  var data1State: ValueState[D1] = _
  var data2State: ValueState[D2] = _
  var timeState: ValueState[lang.Long] = _


  override def open(parameters: Configuration): Unit = {
    val data1StateDescriptor = new ValueStateDescriptor[D1]("data1-state", implicitly[TypeInformation[D1]])
    val data2StateDescriptor = new ValueStateDescriptor[D2]("data2-state", implicitly[TypeInformation[D2]])
    val (ttlConfig1, ttlConfig2) = ttlConfs
    ttlConfig1.foreach(data1StateDescriptor.enableTimeToLive(_))
    ttlConfig2.foreach(data2StateDescriptor.enableTimeToLive(_))
    data1State = getRuntimeContext.getState(data1StateDescriptor)
    data2State = getRuntimeContext.getState(data2StateDescriptor)
    timeState = getRuntimeContext.getState(new ValueStateDescriptor[lang.Long]("time-state", classOf[lang.Long]))
  }

  override def processElement(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    if (value._2.isDefined) {
      processElement1(value._2.get, ctx, out)
    }
    if (value._3.isDefined) {
      processElement2(value._3.get, ctx, out)
    }
  }

  def processElement1(data1: D1, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    val hisData1 = data1State.value()
    val (isReplace, isHandle) = if(hisData1 == null) (true, true) else isReplaceAndHandleData1(data1, hisData1)
    if(isReplace){
      data1State.update(data1)
    }
    if (!isReplace || !isHandle) {
      return
    }

    val data2 = data2State.value()
    if (data2 != null) {
      out.collect((ctx.getCurrentKey, Some(data1), Some(data2)))

      if (timeState.value() != null) {
        ctx.timerService().deleteProcessingTimeTimer(timeState.value())
        timeState.clear()
      }
    } else if (timeState.value() == null) {
      val time = ctx.timerService().currentProcessingTime() + delayMs
      ctx.timerService().registerProcessingTimeTimer(time)
      timeState.update(time)
    }
  }

  def processElement2(data2: D2, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    val hisData2 = data2State.value()
    val (isReplace, isHandle) = if(hisData2 == null) (true, true) else isReplaceAndHandleData2(data2, hisData2)
    if(isReplace){
      data2State.update(data2)
    }
    if (!isReplace || !isHandle) {
      return
    }

    val data1 = data1State.value()
    if (data1 != null) {
      out.collect((ctx.getCurrentKey, Some(data1), Some(data2)))

      if (timeState.value() != null) {
        ctx.timerService().deleteProcessingTimeTimer(timeState.value())
        timeState.clear()
      }
    } else if (timeState.value() == null) {
      val time = ctx.timerService().currentProcessingTime() + delayMs
      ctx.timerService().registerProcessingTimeTimer(time)
      timeState.update(time)
    }
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[K, I, O]#OnTimerContext, out: Collector[O]): Unit = {
    timeState.clear()
    val data1 = data1State.value()
    val data2 = data2State.value()
    if (data1 != null && data2 != null) {

    } else {
      out.collect((ctx.getCurrentKey, Option(data1), Option(data2)))
    }
  }
}

object Unoin2JoinKeyedProcessFunction {

  def union[D1, D2, K](ds1: DataStream[D1], ds2: DataStream[D2])(getKey1: D1 => K, getKey2: D2 => K)
    (implicit typeInfo:TypeInformation[(K, Option[D1], Option[D2])], keyTypeInfo:TypeInformation[K]): KeyedStream[(K, Option[D1], Option[D2]), K] ={
    ds1.map(x => (getKey1(x), Option(x), None: Option[D2]))(typeInfo).setParallelism(ds1.parallelism).union(
      ds2.map(x => (getKey2(x), None: Option[D1], Option(x)))(typeInfo).setParallelism(ds2.parallelism)
    ).keyBy(_._1)(keyTypeInfo)
  }

}