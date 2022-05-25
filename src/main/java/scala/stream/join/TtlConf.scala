package scala.stream.join

import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time

object TtlConf{
  def apply(
    ttlMs: Long = 60000,
    updateType: StateTtlConfig.UpdateType = StateTtlConfig.UpdateType.OnReadAndWrite, // 这里设置OnReadAndWrite，具体根据业务情况设置
    stateVisibility: StateTtlConfig.StateVisibility = StateTtlConfig.StateVisibility.NeverReturnExpired
  ): Option[StateTtlConfig] = {
    val ttlConfig = StateTtlConfig
      .newBuilder(Time.milliseconds(ttlMs))
      .setUpdateType(updateType)
      .setStateVisibility(stateVisibility)
      .build
    Some(ttlConfig)
  }
}

