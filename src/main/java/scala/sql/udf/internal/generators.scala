package scala.sql.udf.internal

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{ArrayData, GenericRowData, MapData}
import org.apache.flink.table.functions.FunctionContext
import org.apache.flink.table.types.{CollectionDataType, DataType, KeyValueDataType}
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.LogicalTypeRoot.{ARRAY, MAP, MULTISET}

import InternalScalarFunction._

abstract class ExplodeBase extends InternalTableFunction {
  var dataType: DataType = _
  var out: AnyRef => Unit = _

  def position: Boolean

  override def open(context: FunctionContext): Unit = {
    out = dataType.getLogicalType.getTypeRoot match {
      case ARRAY =>
        val dType = dataType.asInstanceOf[CollectionDataType].getElementDataType
        val eleGetter = ArrayData.createElementGetter(dType.getLogicalType)
        x => {
          val array = x.asInstanceOf[ArrayData]
          var i = 0
          while (i < array.size()) {
            val value = eleGetter.getElementOrNull(array, i)
            if (position) {
              collect(GenericRowData.of(i:Integer, value))
            } else {
              collect(GenericRowData.of(value))
            }
            i += 1
          }
        }
      case MAP =>
        val keyType = dataType.asInstanceOf[KeyValueDataType].getKeyDataType
        val valueType = dataType.asInstanceOf[KeyValueDataType].getValueDataType
        val ketGetter = ArrayData.createElementGetter(keyType.getLogicalType)
        val valueGetter = ArrayData.createElementGetter(valueType.getLogicalType)
        x => {
          val map = x.asInstanceOf[MapData]
          val keyArray = map.keyArray()
          val valueArray = map.valueArray()
          var i = 0
          while (i < map.size()) {
            val key = ketGetter.getElementOrNull(keyArray, i)
            val value = valueGetter.getElementOrNull(valueArray, i)
            if (position) {
              collect(GenericRowData.of(i:Integer, key, value))
            } else {
              collect(GenericRowData.of(key, value))
            }
            i += 1
          }
        }
      case MULTISET =>
        val dType = dataType.asInstanceOf[CollectionDataType].getElementDataType
        val valueGetter = ArrayData.createElementGetter(dType.getLogicalType)
        x => {
          val map = x.asInstanceOf[MapData]
          val valueArray = map.keyArray()
          val cntArray = map.valueArray()
          var count = 0
          var i = 0
          while (i < map.size()) {
            val value = valueGetter.getElementOrNull(valueArray, i)
            val cnt = cntArray.getInt(i)
            var j = 0
            while(j < cnt){
              if (position) {
                collect(GenericRowData.of(count:Integer, value))
              } else {
                collect(GenericRowData.of(value))
              }
              count += 1
              j += 1
            }

            i += 1
          }
        }
    }
  }

  def eval(value: AnyRef): Unit = {
    if (value == null) {
      return
    }

    out(value)
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(1)

  override def stringArgs: Seq[String] = Seq("value")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    val dType = args(0).getLogicalType.getTypeRoot
    if (dType != ARRAY && dType != MAP && dType != MULTISET) {
      throw callContext.newValidationError(s"input to function explode should be array or map type, not $dType")
    }
    args
  }

  override def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    val arg = args(0)
    dataType = arg
    arg.getLogicalType.getTypeRoot match {
      case ARRAY =>
        val dType = arg.asInstanceOf[CollectionDataType].getElementDataType
        if (position) {
          DataTypes.ROW(
            DataTypes.FIELD("pos", DataTypes.INT()),
            DataTypes.FIELD("col", dType)
          )
        } else {
          DataTypes.ROW(DataTypes.FIELD("col", dType))
        }
      case MAP =>
        val mapType = arg.asInstanceOf[KeyValueDataType]
        val keyType = mapType.getKeyDataType
        val valueType = mapType.getValueDataType
        if (position) {
          DataTypes.ROW(
            DataTypes.FIELD("pos", DataTypes.INT()),
            DataTypes.FIELD("key", keyType),
            DataTypes.FIELD("value", valueType)
          )
        } else {
          DataTypes.ROW(
            DataTypes.FIELD("key", keyType),
            DataTypes.FIELD("value", valueType)
          )
        }
      case MULTISET =>
        val dType = arg.asInstanceOf[CollectionDataType].getElementDataType
        if (position) {
          DataTypes.ROW(
            DataTypes.FIELD("pos", DataTypes.INT()),
            DataTypes.FIELD("col", dType)
          )
        } else {
          DataTypes.ROW(DataTypes.FIELD("col", dType))
        }
    }
  }
}

class Explode extends ExplodeBase {
  override def position: Boolean = false
}

class PosExplode extends ExplodeBase {
  override def position: Boolean = true
}