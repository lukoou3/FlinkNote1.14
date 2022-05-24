package scala.stream.func

import java.lang

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

/**
 * 每个元素最多每隔delayMs输出一次最新的数据
 *
 * @param intervalMs 周期，单位毫秒
 * @param isEager    是否急切模式
 *                   true：元素第一次来时就立即发送，之后过了interval后才会再次发送最新的元素。
 *                      实现相对复杂，使用第二种实现简单的方案：
 *                          1、元素第一次来时发送并记录时间，元素再来时判断是否发送还是定时发送
 *                          2、来了元素看是否有定时器，没定时器就立刻发送设置定时，有定时器就更新元素，等定时到了发送元素(期间有新的元素来)并清空定时
 *                   false: 元素第一次来时过interval后发送最新的元素，interval清除后重复此步骤。
 *                      实现简单：来了元素判断是否定时就行
 * @param isNewData  是否是新的元素，旧的元素直接忽略
 */
class KeyedIntervalSendProcessFunc[K, T: TypeInformation](
  intervalMs: Long,
  isEager: Boolean = true,
  isNewData: (T, T) => Boolean = (newData: T, oldData: T) => true
) extends KeyedProcessFunction[K, T, T] {
  @transient var dataState: ValueState[T] = _ // 数据
  @transient var timeState: ValueState[lang.Long] = _ // 设置的定时器时间
  @transient var firstDelayTs = 0L //首次延时未发送元素的时间，用于验证程序逻辑正确性

  override def open(parameters: Configuration): Unit = {
    dataState = getRuntimeContext.getState(new ValueStateDescriptor[T]("data-state", createTypeInformation[T]))
    timeState = getRuntimeContext.getState(new ValueStateDescriptor[lang.Long]("time-state", createTypeInformation[lang.Long]))
  }

  def verifyDelay(): Unit = {
    if (firstDelayTs <= 0L) {
      firstDelayTs = System.currentTimeMillis()
    } else {
      if (System.currentTimeMillis() - firstDelayTs > intervalMs * 2) {
        throw new Exception("逻辑错误或者定时器延时严重")
      }
    }
  }

  override def processElement(data: T, ctx: KeyedProcessFunction[K, T, T]#Context, out: Collector[T]): Unit = {
    val preData = dataState.value()

    if (isEager) {
      if (timeState.value() == null) {
        out.collect(data)
        // 下次允许发送元素的最早时间
        val time = ctx.timerService().currentProcessingTime() + intervalMs
        ctx.timerService().registerProcessingTimeTimer(time)
        timeState.update(time)
      } else {
        if (preData == null || isNewData(data, preData)) {
          verifyDelay()
          dataState.update(data)
        }
      }

    } else {
      if (preData == null || isNewData(data, preData)) {
        dataState.update(data)

        // 设置过了就不用再设置, 等输出后timeState会清空需要重新设置
        if (timeState.value() == null) {
          // 发送元素的时间
          val time = ctx.timerService().currentProcessingTime() + intervalMs
          ctx.timerService().registerProcessingTimeTimer(time)
          timeState.update(time)
        } else {
          verifyDelay()
        }
      }
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[K, T, T]#OnTimerContext, out: Collector[T]): Unit = {
    val data = dataState.value()

    if (isEager) {
      if (data != null) {
        out.collect(data)
      }
    } else {
      out.collect(data)
    }

    dataState.clear()
    timeState.clear()

    firstDelayTs = 0L
  }
}
