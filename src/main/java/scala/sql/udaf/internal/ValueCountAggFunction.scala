package scala.sql.udaf.internal

import java.util.Objects

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{GenericMapData, MapData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import scala.sql.udf.internal.InternalScalarFunction._
import ValueCountAggFunction._

abstract class ValueCountAggFunction extends InternalAggFunction[MapData, Accumulator] {
  def useLong: Boolean

  def createAccumulator(): Accumulator = {
    val acc = new Accumulator
    acc.map = new MapView[Any, Any]
    acc
  }

  def accumulate(acc: Accumulator, value: Object): Unit = {
    if(value != null){
      val map = acc.map
      val count = map.get(value)
      if(count == null){
        if(useLong){
          map.put(value, 1L)
        }else{
          map.put(value, 1)
        }
      }else{
         if(useLong){
           map.put(value,count.asInstanceOf[Long] + 1L)
         } else{
           map.put(value,count.asInstanceOf[Int] + 1)
         }
      }

    }
  }

  def accumulate(acc: Accumulator, value: Object, useBigint: Int): Unit = {
    accumulate(acc, value)
  }

  def getValue(acc: Accumulator): MapData = {
    new GenericMapData(acc.map.getMap)
  }

  def argumentCount: ArgumentCount = betweenArgumentCount(1, 2)

  def stringArgs: Seq[String] = Seq("value", "useLong")

  def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = expectArgs(args, callContext)

  def inferAccumulatorDataType(args: Seq[DataType], callContext: CallContext): DataType = {
    DataTypes.STRUCTURED(
      classOf[Accumulator],
      DataTypes.FIELD("map", MapView.newMapViewDataType(args(0).notNull(), if(useLong) DataTypes.BIGINT() else DataTypes.INT()))
    )
  }

  def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    DataTypes.MAP(args(0).notNull(), if(useLong) DataTypes.BIGINT() else DataTypes.INT())
  }

  def expectArgs(args: Seq[DataType], callContext: CallContext): Seq[DataType] ={
    if(args.length > 1){
      assert(args(1).getLogicalType.getTypeRoot == INTEGER)
      /*assert(args(1).getLogicalType.getTypeRoot, callContext.isArgumentLiteral(1))
      useLong = callContext.getArgumentValue[Integer](1, classOf[Integer]).get() > 0*/
    }
    args
  }

}

class ValueCountIntAggFunction extends ValueCountAggFunction{
  def useLong: Boolean = false
}

class ValueCountBigintAggFunction extends ValueCountAggFunction{
  def useLong: Boolean = true
}


object ValueCountAggFunction{
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