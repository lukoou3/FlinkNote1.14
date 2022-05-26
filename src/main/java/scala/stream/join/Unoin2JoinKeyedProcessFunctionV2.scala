package scala.stream.join

import java.lang

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}
import org.apache.flink.util.Collector

import Unoin2JoinKeyedProcessFunctionV2._
import scala.beans.BeanProperty

/**
 * 二流join的版本2, 考虑对象的重用, 上游构造的union ds涉及到shuffle不需要新建对象
 * onTimer中输出的元素涉及到对象的重用，感觉默认开启就行，一般下游不会直接使用这个Unoin2Data对象，会做转换
 *
 * @param ttlConfs                  ValueState的ttl失效时间
 * @param delayMs                   流中元素等待的最长时间,超时就输出
 * @param isReplaceAndHandleData1   replace:Boolean = 是否比上一个数据新, handle:Boolean = 是否处理数据
 * @param isReplaceAndHandleData2   replace:Boolean = 是否比上一个数据新, handle:Boolean = 是否处理数据
 * @param objectReuse               对象重用, 复用onTimer中使用的输出对象
 * @tparam K  key
 * @tparam D1 data1
 * @tparam D2 data2
 */
class Unoin2JoinKeyedProcessFunctionV2[K, D1: TypeInformation, D2: TypeInformation](
  ttlConfs: (Option[StateTtlConfig], Option[StateTtlConfig]) = (TtlConf(), TtlConf()),
  delayMs: Long = 3000,
  isReplaceAndHandleData1: IsReplaceAndHandleData[D1] = defaultIsReplaceAndHandleData[D1],
  isReplaceAndHandleData2: IsReplaceAndHandleData[D2] = defaultIsReplaceAndHandleData[D2],
  objectReuse: Boolean = true // 复用对象, 默认开启, 主要用于延时定时对象的发送
) extends KeyedProcessFunction[K, Unoin2Data[K, D1, D2], Unoin2Data[K, D1, D2]] {
  type I = Unoin2Data[K, D1, D2]
  type O = Unoin2Data[K, D1, D2]
  var data1State: ValueState[D1] = _
  var data2State: ValueState[D2] = _
  var timeState: ValueState[lang.Long] = _
  var tip: ReplaceAndHandleDataTip = _
  var outData: O = _

  override def open(parameters: Configuration): Unit = {
    val data1StateDescriptor = new ValueStateDescriptor[D1]("data1-state", implicitly[TypeInformation[D1]])
    val data2StateDescriptor = new ValueStateDescriptor[D2]("data2-state", implicitly[TypeInformation[D2]])
    val (ttlConfig1, ttlConfig2) = ttlConfs
    ttlConfig1.foreach(data1StateDescriptor.enableTimeToLive(_))
    ttlConfig2.foreach(data2StateDescriptor.enableTimeToLive(_))
    data1State = getRuntimeContext.getState(data1StateDescriptor)
    data2State = getRuntimeContext.getState(data2StateDescriptor)
    timeState = getRuntimeContext.getState(new ValueStateDescriptor[lang.Long]("time-state", classOf[lang.Long]))
    tip = new ReplaceAndHandleDataTip
    if(objectReuse){
      outData = new Unoin2Data[K, D1, D2]()
    }
  }

  def processElement(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    if(value.data1 != null){
      processElement1(value, ctx, out)
    }else if(value.data2 != null){ // 这个必须用else if,processElement1可能为data2赋值,这样写没问题:data1和data2不可能同时不为null
      processElement2(value, ctx, out)
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
    if (data2 != null) {
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
    if (data1 != null) {
      value.data1 = data1
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
    if (data1 != null && data2 != null) {

    } else {
      if(!objectReuse){
        outData = new Unoin2Data[K, D1, D2]()
      }

      outData.setKey(ctx.getCurrentKey)
      outData.setData1(data1)
      outData.setData2(data2)
      out.collect(outData)
    }
  }
}

object Unoin2JoinKeyedProcessFunctionV2{
  type IsReplaceAndHandleData[D] = (D, D, ReplaceAndHandleDataTip) => Unit

  def defaultIsReplaceAndHandleData[D]:IsReplaceAndHandleData[D] = (newData:D, oldData:D, tip: ReplaceAndHandleDataTip) => {}

  def union[D1, D2, K](ds1: DataStream[D1], ds2: DataStream[D2])(getKey1: D1 => K, getKey2: D2 => K)
    (implicit typeInfo:TypeInformation[Unoin2Data[K, D1, D2]], keyTypeInfo:TypeInformation[K]): KeyedStream[Unoin2Data[K, D1, D2], K] ={
    ds1.map(new MapFunction[D1, Unoin2Data[K, D1, D2]]{
      val data = new Unoin2Data[K, D1, D2]
      def map(value: D1): Unoin2Data[K, D1, D2] = {
        data.key = getKey1(value)
        data.data1 = value
        data
      }
    })(typeInfo).setParallelism(ds1.parallelism).union(
      ds2.map(new MapFunction[D2, Unoin2Data[K, D1, D2]]{
        val data = new Unoin2Data[K, D1, D2]
        def map(value: D2): Unoin2Data[K, D1, D2] = {
          data.key = getKey2(value)
          data.data2 = value
          data
        }
      })(typeInfo).setParallelism(ds2.parallelism)
    ).keyBy(_.key)(keyTypeInfo)
  }

  class ReplaceAndHandleDataTip private[Unoin2JoinKeyedProcessFunctionV2] extends Serializable {
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

  class Unoin2Data[K, D1, D2] extends Serializable{
    @BeanProperty
    var key: K = _
    @BeanProperty
    var data1: D1 = _
    @BeanProperty
    var data2: D2 = _

    def this(key: K, data1: D1, data2: D2){
      this()
      this.key = key
      this.data1 = data1
      this.data2 = data2
    }

  }

}