package scala.stream.join

import java.lang

import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * 自定义函数实现三流join, scala版本，需引入隐式转换
 * @param ttlMs ValueState的ttl失效时间
 * @param delayMs 流中元素等待的最长时间,超时就输出
 * @param noProcessSameValue 如果data和上个ValueState的值相等则不输出
 * @tparam K key
 * @tparam D1 data1
 * @tparam D2 data2
 * @tparam D3 data3
 */
class Unoin3JoinKeyedProcessFunction[K, D1: TypeInformation, D2: TypeInformation, D3: TypeInformation]
  (ttlMs: Long = 60000, delayMs: Long = 3000, noProcessSameValue: (Boolean, Boolean, Boolean) =(false, false, false))
  extends KeyedProcessFunction[K, (K, Option[D1], Option[D2], Option[D3]), (K, Option[D1], Option[D2], Option[D3])] {
  type I = (K, Option[D1], Option[D2], Option[D3])
  type O = (K, Option[D1], Option[D2], Option[D3])
  var data1State: ValueState[D1] = _
  var data2State: ValueState[D2] = _
  var data3State: ValueState[D3] = _
  var timeState: ValueState[lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    // 这里设置OnReadAndWrite，具体根据业务情况设置
    val ttlConfig = StateTtlConfig
      .newBuilder(Time.milliseconds(ttlMs))
      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    val data1StateDescriptor = new ValueStateDescriptor[D1]("data1-state", implicitly[TypeInformation[D1]])
    val data2StateDescriptor = new ValueStateDescriptor[D2]("data2-state", implicitly[TypeInformation[D2]])
    val data3StateDescriptor = new ValueStateDescriptor[D3]("data3-state", implicitly[TypeInformation[D3]])
    data1StateDescriptor.enableTimeToLive(ttlConfig)
    data2StateDescriptor.enableTimeToLive(ttlConfig)
    data3StateDescriptor.enableTimeToLive(ttlConfig)
    data1State = getRuntimeContext.getState(data1StateDescriptor)
    data2State = getRuntimeContext.getState(data2StateDescriptor)
    data3State = getRuntimeContext.getState(data3StateDescriptor)
    timeState = getRuntimeContext.getState(new ValueStateDescriptor[lang.Long]("time-state", classOf[lang.Long]))
  }

  override def processElement(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    if (value._2.isDefined) {
      processElement1(value._2.get, ctx, out)
    }
    if (value._3.isDefined) {
      processElement2(value._3.get, ctx, out)
    }
    if (value._4.isDefined) {
      processElement3(value._4.get, ctx, out)
    }
  }

  def processElement1(data1: D1, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    val hisData1 = data1State.value()
    data1State.update(data1)
    // 如果这次能关联完全(data2 != null && data3 != null)，之前也肯定输出过一次(三个State都有值的条件已满足会立即出发输出)
    if(noProcessSameValue._1 && hisData1 == data1){
      return
    }

    val data2 = data2State.value()
    val data3 = data3State.value()
    if (data2 != null && data3 != null) {
      out.collect((ctx.getCurrentKey, Some(data1), Some(data2), Some(data3)))

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
    data2State.update(data2)
    if(noProcessSameValue._2 && hisData2 == data2){
      return
    }

    val data1 = data1State.value()
    val data3 = data3State.value()
    if (data1 != null && data3 != null) {
      out.collect((ctx.getCurrentKey, Some(data1), Some(data2), Some(data3)))

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

  def processElement3(data3: D3, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    val hisData3 = data3State.value()
    data3State.update(data3)
    if(noProcessSameValue._3 && hisData3 == data3){
      return
    }

    val data1 = data1State.value()
    val data2 = data2State.value()
    if (data1 != null && data2 != null) {
      out.collect((ctx.getCurrentKey, Some(data1), Some(data2), Some(data3)))

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
    val data3 = data3State.value()
    if (data1 != null && data2 != null && data3 != null) {

    } else {
      out.collect((ctx.getCurrentKey, Option(data1), Option(data2), Option(data3)))
    }
  }
}
