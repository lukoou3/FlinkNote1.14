package scala.sql.udaf.internal

import java.util.Objects

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{GenericMapData, MapData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import scala.sql.udf.internal.InternalScalarFunction._
import ValueSumAggFunction._

class ValueSumAggFunction extends InternalAggFunction[MapData, Accumulator] {
  var valueLogicalType: LogicalType = _
  @transient lazy val sumValue: (Any, Any) => Any = valueLogicalType.getTypeRoot match {
    case BIGINT => (a, b) => a.asInstanceOf[Long] + b.asInstanceOf[Long]
    case DOUBLE => (a, b) => a.asInstanceOf[Double] + b.asInstanceOf[Double]
  }

  def createAccumulator(): Accumulator = {
    val acc = new Accumulator
    acc.map = new MapView[Any, Any]
    acc
  }

  def accumulate(acc: Accumulator, key: Object, value: Object): Unit = {
    if(key != null && value != null){
      val map = acc.map
      val sum = map.get(key)
      if(sum == null){
        map.put(key, value)
      }else{
        map.put(key, sumValue(sum, value))
      }
    }
  }

  def getValue(acc: Accumulator): MapData = {
    new GenericMapData(acc.map.getMap)
  }

  def argumentCount: ArgumentCount = fixArgumentCount(2)

  def stringArgs: Seq[String] = Seq("key", "value")

  def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    expectArgs(args)
  }

  def inferAccumulatorDataType(args: Seq[DataType], callContext: CallContext): DataType = {
    val Seq(arg1, arg2) = expectArgs(args)
    DataTypes.STRUCTURED(
      classOf[Accumulator],
      DataTypes.FIELD("map", MapView.newMapViewDataType(arg1.notNull(), arg2))
    )
  }

  def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    val Seq(arg1, arg2) = expectArgs(args)
    DataTypes.MAP(arg1, arg2)
  }

  def expectArgs(args: Seq[DataType]): Seq[DataType] ={
    val arg1 = args(0)
    val arg2 = args(1).getLogicalType.getTypeRoot match {
      case INTEGER | BIGINT => DataTypes.BIGINT()
      case FLOAT | DOUBLE => DataTypes.DOUBLE()
    }
    valueLogicalType = arg2.getLogicalType
    Seq(arg1, arg2)
  }
}

object ValueSumAggFunction {

  class Accumulator {
    var map: MapView[Any, Any] = null

    override def equals(o: Any): Boolean = {
      if (this == o) {
        return true
      }
      if (o == null || getClass() != o.getClass()) {
        return false
      }
      val that = o.asInstanceOf[Accumulator]
      Objects.equals(map, that.map)
    }
  }

}
