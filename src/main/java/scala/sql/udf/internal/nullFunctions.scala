package scala.sql.udf.internal

import org.apache.flink.table.functions.FunctionContext
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}

import InternalScalarFunction._

class Nvl extends InternalScalarFunction {
  var dataType: DataType = _

  override def open(context: FunctionContext): Unit = {
    assert(dataType != null)
  }

  def eval(left: AnyRef, right: AnyRef): AnyRef = {
    if (left != null) left else right
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(2)

  override def stringArgs: Seq[String] = Seq("left", "right")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    val optionDataType = findTightestCommonType(args(0), args(1))
    val dataType = optionDataType.getOrElse(throw new Exception("must same type"))
    Seq(dataType, dataType)
  }

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext): DataType = {
    val optionDataType = findTightestCommonType(args(0), args(1))
    dataType = optionDataType.getOrElse(throw new Exception("must same type"))
    dataType
  }

}
