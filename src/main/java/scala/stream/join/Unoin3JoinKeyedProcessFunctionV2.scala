package scala.stream.join

import java.lang

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}
import org.apache.flink.util.Collector

import scala.beans.BeanProperty
import scala.stream.join.Unoin3JoinKeyedProcessFunctionV2._

class Unoin3JoinKeyedProcessFunctionV2[K, D1: TypeInformation, D2: TypeInformation, D3: TypeInformation](
  ttlConfs: (Option[StateTtlConfig], Option[StateTtlConfig], Option[StateTtlConfig]) = (TtlConf(), TtlConf(), TtlConf()),
  delayMs: Long = 3000,
  isReplaceAndHandleData1: IsReplaceAndHandleData[D1] = defaultIsReplaceAndHandleData[D1],
  isReplaceAndHandleData2: IsReplaceAndHandleData[D2] = defaultIsReplaceAndHandleData[D2],
  isReplaceAndHandleData3: IsReplaceAndHandleData[D3] = defaultIsReplaceAndHandleData[D3],
  objectReuse: Boolean = true // 复用对象, 默认开启, 主要用于延时定时对象的发送
) extends KeyedProcessFunction[K, Unoin3Data[K, D1, D2, D3], Unoin3Data[K, D1, D2, D3]]{
  type I = Unoin3Data[K, D1, D2, D3]
  type O = Unoin3Data[K, D1, D2, D3]
  var data1State: ValueState[D1] = _
  var data2State: ValueState[D2] = _
  var data3State: ValueState[D3] = _
  var timeState: ValueState[lang.Long] = _
  var tip: ReplaceAndHandleDataTip = _
  var outData: O = _

  override def open(parameters: Configuration): Unit = {
    val data1StateDescriptor = new ValueStateDescriptor[D1]("data1-state", implicitly[TypeInformation[D1]])
    val data2StateDescriptor = new ValueStateDescriptor[D2]("data2-state", implicitly[TypeInformation[D2]])
    val data3StateDescriptor = new ValueStateDescriptor[D3]("data3-state", implicitly[TypeInformation[D3]])
    val (ttlConfig1, ttlConfig2, ttlConfig3) = ttlConfs
    ttlConfig1.foreach(data1StateDescriptor.enableTimeToLive(_))
    ttlConfig2.foreach(data2StateDescriptor.enableTimeToLive(_))
    ttlConfig3.foreach(data3StateDescriptor.enableTimeToLive(_))
    data1State = getRuntimeContext.getState(data1StateDescriptor)
    data2State = getRuntimeContext.getState(data2StateDescriptor)
    data3State = getRuntimeContext.getState(data3StateDescriptor)
    timeState = getRuntimeContext.getState(new ValueStateDescriptor[lang.Long]("time-state", classOf[lang.Long]))
    tip = new ReplaceAndHandleDataTip
    if(objectReuse){
      outData = new Unoin3Data[K, D1, D2, D3]()
    }
  }

  def processElement(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    if(value.data1 != null){
      processElement1(value, ctx, out)
    }else if(value.data2 != null){ // 这个必须用else if,processElement1可能为data2赋值,这样写没问题:data1和data2不可能同时不为null
      processElement2(value, ctx, out)
    }else if(value.data3 != null){
      processElement3(value, ctx, out)
    }
  }

  def processElement1(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    val data1 = value.data1
    val hisData1 = data1State.value()
    tip.reSetReplaceAndHandleTrue()
    if(hisData1 != null){
      isReplaceAndHandleData1(data1, hisData1, tip)
    }
    if(tip.replace){
      data1State.update(data1)
    }
    if(!tip.replace || !tip.handle){
      return
    }

    val data2 = data2State.value()
    val data3 = data3State.value()
    if (data2 != null && data3 != null) {
      value.data2 = data2
      value.data3 = data3
      out.collect(value)

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

  def processElement2(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    val data2 = value.data2
    val hisData2 = data2State.value()
    tip.reSetReplaceAndHandleTrue()
    if(hisData2 != null){
      isReplaceAndHandleData2(data2, hisData2, tip)
    }
    if(tip.replace){
      data2State.update(data2)
    }
    if(!tip.replace || !tip.handle){
      return
    }

    val data1 = data1State.value()
    val data3 = data3State.value()
    if (data1 != null && data3 != null) {
      value.data1 = data1
      value.data3 = data3
      out.collect(value)

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

  def processElement3(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    val data3 = value.data3
    val hisData3 = data3State.value()
    tip.reSetReplaceAndHandleTrue()
    if(hisData3 != null){
      isReplaceAndHandleData3(data3, hisData3, tip)
    }
    if(tip.replace){
      data3State.update(data3)
    }
    if(!tip.replace || !tip.handle){
      return
    }

    val data1 = data1State.value()
    val data2 = data2State.value()
    if (data1 != null && data2 != null) {
      value.data1 = data1
      value.data2 = data2
      out.collect(value)

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
      if(!objectReuse){
        outData = new Unoin3Data[K, D1, D2, D3]()
      }

      outData.setKey(ctx.getCurrentKey)
      outData.setData1(data1)
      outData.setData2(data2)
      outData.setData3(data3)
      out.collect(outData)
    }
  }
}

object Unoin3JoinKeyedProcessFunctionV2 {
  type IsReplaceAndHandleData[D] = (D, D, ReplaceAndHandleDataTip) => Unit

  def defaultIsReplaceAndHandleData[D]:IsReplaceAndHandleData[D] = (newData:D, oldData:D, tip: ReplaceAndHandleDataTip) => {}

  def union[D1, D2, D3, K](ds1: DataStream[D1], ds2: DataStream[D2], ds3: DataStream[D3])(getKey1: D1 => K, getKey2: D2 => K, getKey3: D3 => K)
    (implicit typeInfo:TypeInformation[Unoin3Data[K, D1, D2, D3]], keyTypeInfo:TypeInformation[K]): KeyedStream[Unoin3Data[K, D1, D2, D3], K] ={
    ds1.map(new MapFunction[D1, Unoin3Data[K, D1, D2, D3]]{
      val data = new Unoin3Data[K, D1, D2, D3]
      def map(value: D1): Unoin3Data[K, D1, D2, D3] = {
        data.key = getKey1(value)
        data.data1 = value
        data
      }
    })(typeInfo).setParallelism(ds1.parallelism).union(
      ds2.map(new MapFunction[D2, Unoin3Data[K, D1, D2, D3]]{
        val data = new Unoin3Data[K, D1, D2, D3]
        def map(value: D2): Unoin3Data[K, D1, D2, D3] = {
          data.key = getKey2(value)
          data.data2 = value
          data
        }
      })(typeInfo).setParallelism(ds2.parallelism),
      ds3.map(new MapFunction[D3, Unoin3Data[K, D1, D2, D3]]{
        val data = new Unoin3Data[K, D1, D2, D3]
        def map(value: D3): Unoin3Data[K, D1, D2, D3] = {
          data.key = getKey3(value)
          data.data3 = value
          data
        }
      })(typeInfo).setParallelism(ds3.parallelism)
    ).keyBy(_.key)(keyTypeInfo)
  }

  class ReplaceAndHandleDataTip private[Unoin3JoinKeyedProcessFunctionV2] extends Serializable {
    var replace:Boolean = false
    var handle:Boolean = false

    def setReplaceAndHandle(replace:Boolean, handle:Boolean): Unit ={
      this.replace = replace
      this.handle = handle
    }

    def reSetReplaceAndHandleFalse(): Unit ={
      this.replace = false
      this.handle = false
    }

    def reSetReplaceAndHandleTrue(): Unit ={
      this.replace = true
      this.handle = true
    }
  }

  class Unoin3Data[K, D1, D2, D3] extends Serializable{
    @BeanProperty
    var key: K = _
    @BeanProperty
    var data1: D1 = _
    @BeanProperty
    var data2: D2 = _
    @BeanProperty
    var data3: D3 = _

    def this(key: K, data1: D1, data2: D2, data3: D3){
      this()
      this.key = key
      this.data1 = data1
      this.data2 = data2
      this.data3 = data3
    }

  }
}