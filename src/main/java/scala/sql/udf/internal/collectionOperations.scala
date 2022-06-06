package scala.sql.udf.internal

import java.util.Comparator

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{ArrayData, GenericArrayData, MapData, RowData, StringData}
import org.apache.flink.table.types.{CollectionDataType, DataType}
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.{ArrayType, LogicalType, RowType, VarCharType}
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import InternalScalarFunction._
import scala.collection.mutable

class Size extends InternalScalarFunction{
  var logicalType: LogicalType = _

  def eval(value: AnyRef): Int = {
    if(value == null){
      return -1
    }
    logicalType.getTypeRoot match {
      case ARRAY => value.asInstanceOf[ArrayData].size()
      case MAP => value.asInstanceOf[MapData].size()
      case MULTISET => value.asInstanceOf[MapData].size()
    }
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(1)

  override def stringArgs: Seq[String] = Seq("collection")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    val dType = args(0).getLogicalType.getTypeRoot
    if (dType != ARRAY && dType != MAP && dType != MULTISET) {
      throw callContext.newValidationError(s"input to function size should be array or map type, not $dType")
    }
    logicalType = args(0).getLogicalType
    args
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = DataTypes.INT()
}

class Slice extends InternalScalarFunction{
  var logicalType: LogicalType = _
  @transient private lazy val eleGetter = ArrayData.createElementGetter(logicalType)

  def eval(array: ArrayData, start:Int, length:Int): ArrayData = {
    if(array == null){
      return null
    }

    val startIndex = if (start == 0) {
      throw new RuntimeException(
        s"Unexpected value for start in function slice: SQL array indices start at 1.")
    } else if (start < 0) {
      start + array.size()
    } else {
      start - 1
    }
    if (length < 0) {
      throw new RuntimeException(s"Unexpected value for length in function slice: " +
        "length must be greater than or equal to 0.")
    }
    // startIndex can be negative if start is negative and its absolute value is greater than the
    // number of elements in the array
    if (startIndex < 0 || startIndex >= array.size()) {
      return new GenericArrayData(Array.empty[AnyRef])
    }

    val values = new Array[AnyRef](math.min(length, array.size() - startIndex))
    var i = startIndex
    while(i < values.length){
      val value = eleGetter.getElementOrNull(array, i)
      values(i) = value
      i += 1
    }

    new GenericArrayData(values)
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(3)

  override def stringArgs: Seq[String] = Seq("x", "start", "length")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    if(args(0).getLogicalType.getTypeRoot != ARRAY){
      throw callContext.newValidationError("参数必须是array")
    }
    logicalType = args(0).asInstanceOf[CollectionDataType].getElementDataType.getLogicalType
    Seq(args(0), DataTypes.INT(), DataTypes.INT())
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = args(0)
}

class ArrayDistinct extends InternalScalarFunction{
  var logicalType: LogicalType = _
  @transient private lazy val eleGetter = ArrayData.createElementGetter(logicalType)

  def eval(obj: AnyRef): ArrayData = {
    if(obj == null){
      return null
    }
    obj match {
      case array: ArrayData =>
        val values = new mutable.HashSet[AnyRef]()
        var i = 0
        while (i < array.size()) {
          val value = eleGetter.getElementOrNull(array, i)
          values += value
          i += 1
        }
        new GenericArrayData(values.toArray)
      case map: MapData =>
        val valueArray = map.keyArray()
        valueArray
    }
  }

  def argumentCount: ArgumentCount = fixArgumentCount(1)

  def stringArgs: Seq[String] = Seq("array|multiset")

  def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    val dType = args(0).getLogicalType.getTypeRoot
    if (dType != ARRAY && dType != MULTISET) {
      throw callContext.newValidationError(s"input to function array_distinct should be array or multiset type, not $dType")
    }
    args
  }

  def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    logicalType = args(0).asInstanceOf[CollectionDataType].getElementDataType.getLogicalType
    new CollectionDataType(new ArrayType(logicalType), args(0).asInstanceOf[CollectionDataType].getElementDataType)
  }
}

class ArrayContains extends InternalScalarFunction {
  var logicalType: LogicalType = _
  @transient private lazy val eleGetter = ArrayData.createElementGetter(logicalType)
  @transient private lazy val ordering = getOrdering(logicalType)

  def eval(array: ArrayData, value: AnyRef): java.lang.Boolean = {
    if(array == null || value == null) {
      return null
    }

    var hasNull = false

    var i = 0
    while (i < array.size()) {
      val v = eleGetter.getElementOrNull(array, i)
      if (v == null) {
        hasNull = true
      } else if (ordering.equiv(v, value)) {
        return true
      }
      i += 1
    }

    if (hasNull) {
      null
    } else {
      false
    }
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(2)

  override def stringArgs: Seq[String] = Seq("arr", "value")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    val dataType = args(0).asInstanceOf[CollectionDataType].getElementDataType
    logicalType = args(0).asInstanceOf[CollectionDataType].getElementDataType.getLogicalType
    Seq(args(0), dataType.nullable())
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = DataTypes.BOOLEAN()
}

class ArrayMax extends InternalScalarFunction{
  var logicalType: LogicalType = _
  @transient private lazy val eleGetter = ArrayData.createElementGetter(logicalType)
  @transient private lazy val ordering = getOrdering(logicalType)

  def eval(array: ArrayData): AnyRef = {
    if(array == null){
      return null
    }

    var max: AnyRef = null
    var i = 0
    while (i < array.size()) {
      val item = eleGetter.getElementOrNull(array, i)
      if (item != null && (max == null || ordering.gt(item, max))) {
        max = item
      }
      i += 1
    }
    max
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(1)

  override def stringArgs: Seq[String] = Seq("array")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    args(0).getLogicalType.getTypeRoot == ARRAY
    args
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    logicalType = args(0).asInstanceOf[CollectionDataType].getElementDataType.getLogicalType
    args(0).asInstanceOf[CollectionDataType].getElementDataType
  }
}

class ArrayMin extends InternalScalarFunction{
  var logicalType: LogicalType = _
  @transient private lazy val eleGetter = ArrayData.createElementGetter(logicalType)
  @transient private lazy val ordering = getOrdering(logicalType)

  def eval(array: ArrayData): AnyRef = {
    if(array == null){
      return null
    }

    var min: AnyRef = null
    var i = 0
    while (i < array.size()) {
      val item = eleGetter.getElementOrNull(array, i)
      if (item != null && (min == null || ordering.lt(item, min))) {
        min = item
      }
      i += 1
    }
    min
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(1)

  override def stringArgs: Seq[String] = Seq("array")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    args(0).getLogicalType.getTypeRoot == ARRAY
    args
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    logicalType = args(0).asInstanceOf[CollectionDataType].getElementDataType.getLogicalType
    args(0).asInstanceOf[CollectionDataType].getElementDataType
  }
}

class SortArray extends InternalScalarFunction {
  var logicalType: LogicalType = _
  @transient private lazy val eleGetter = ArrayData.createElementGetter(logicalType)

  @transient private lazy val lt: Comparator[Any] = {
    val ordering = getOrdering(logicalType)
    new Comparator[Any]{
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          -1
        } else if (o2 == null) {
          1
        } else {
          ordering.compare(o1, o2)
        }
      }
    }
  }

  @transient private lazy val gt: Comparator[Any] = {
    val ordering = getOrdering(logicalType)
    new Comparator[Any]{
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          1
        } else if (o2 == null) {
          -1
        } else {
          ordering.compare(o2, o1)
        }
      }
    }
  }

  def eval(array: ArrayData): ArrayData = {
    eval(array, true)
  }

  def eval(array: ArrayData, ascending: Boolean): ArrayData = {
    if(array == null){
      return null
    }

    val values = new Array[AnyRef](array.size())
    var i = 0
    while (i < array.size()) {
      val value = eleGetter.getElementOrNull(array, i)
      values(i) = value
      i += 1
    }

    java.util.Arrays.sort(values, if (ascending) lt else gt)

    new GenericArrayData(values)
  }

  override def argumentCount: ArgumentCount = betweenArgumentCount(1, 2)

  override def stringArgs: Seq[String] = Seq("array", "[ascendingOrder]")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    args.map(_.getLogicalType.getTypeRoot).zipWithIndex.foreach{ case(dtype, i) =>
      if(i == 0){
        assert(dtype == ARRAY)
      }
      if(i == 1){
        assert(dtype == BOOLEAN)
      }
    }
    args
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    logicalType = args(0).asInstanceOf[CollectionDataType].getElementDataType.getLogicalType
    args(0)
  }
}

