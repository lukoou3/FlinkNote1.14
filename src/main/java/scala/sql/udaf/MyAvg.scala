package scala.sql.udaf

import org.apache.flink.table.functions.AggregateFunction

import scala.sql.udaf.MyAvg.AccData

class MyAvg extends AggregateFunction[Double, AccData]{
  override def createAccumulator(): AccData = ???

  override def getValue(accumulator: AccData): Double = ???

}

object MyAvg{
  case class AccData(
    var sum: Double,
    var count: Int
  )
}
