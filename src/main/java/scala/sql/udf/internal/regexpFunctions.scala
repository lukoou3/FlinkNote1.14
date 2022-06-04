package scala.sql.udf.internal

import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{ArrayData, GenericArrayData, StringData}
import org.apache.flink.table.types.{CollectionDataType, DataType}
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.{ArrayType, VarCharType}

import InternalScalarFunction._

class StringSplit extends InternalScalarFunction {
  @transient private var lastRegex: StringData = _
  @transient private var regexStr: String = _

  def eval(string: StringData, regex: StringData): ArrayData = {
    if (string == null || regex == null) {
      null
    } else {
      if (regex != lastRegex) {
        lastRegex = regex
        regexStr = regex.toString
      }

      val splits = string.toString.split(regexStr, -1)
      val res = new Array[AnyRef](splits.length)

      var i = 0
      while (i < splits.length) {
        res(i) = StringData.fromString(splits(i))
        i += 1
      }

      new GenericArrayData(res)
    }
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(2)

  override def stringArgs: Seq[String] = Seq("string", "regex")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = Seq(stringDateType, stringDateType)

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    new CollectionDataType(new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)), stringDateType)
  }
}

