package scala.stream.func

import org.apache.flink.api.common.functions.MapFunction

/**
 * An MapFunction that forwards all records.
 *
 * Any instance of the function will fail after forwarding a configured number of records by
 * throwing an exception.
 *
 * NOTE: This function is only used to demonstrate Flink's failure recovery capabilities.
 *
 * @param failInterval The number of records that are forwarded before throwing an exception.
 * @tparam IN The type of input and output records.
 */
class FailingMapper[IN](val failInterval: Int) extends MapFunction[IN, IN] {

  var cnt: Int = 0

  override def map(value: IN): IN = {

    cnt += 1
    // check failure condition
    if (cnt > failInterval) {
      throw new RuntimeException("Fail application to demonstrate output consistency.")
    }
    // forward value
    value
  }

}
