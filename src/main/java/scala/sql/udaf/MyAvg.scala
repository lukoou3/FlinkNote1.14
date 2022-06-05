package scala.sql.udaf

import org.apache.flink.table.functions.AggregateFunction

import scala.sql.udaf.MyAvg.AccData

class MyAvg extends AggregateFunction[java.lang.Double, AccData]{
  override def createAccumulator(): AccData = AccData(0, 0)

  def accumulate(acc: AccData, value: java.lang.Double): Unit = {
    if(value != null){
      acc.sum += value
      acc.count += 1
    }
  }


  override def getValue(acc: AccData): java.lang.Double = {
    if(acc.count == 0){
      null
    }else{
      acc.sum / acc.count
    }
  }

}

object MyAvg{
  case class AccData(
    var sum: Double,
    var count: Int
  )
}
