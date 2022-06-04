package scala.sql.udf.internal

import org.apache.flink.table.data.StringData
import org.apache.flink.table.data.binary.{BinaryStringData, BinaryStringDataUtil}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}

import InternalScalarFunction._

class SubstringIndex extends InternalScalarFunction {

  def eval(str: StringData, delim: StringData, count: Int): StringData = {
    if (str == null || delim == null) {
      return null
    }

    if(delim.asInstanceOf[BinaryStringData] == BinaryStringData.EMPTY_UTF8 || count == 0){
      return BinaryStringData.EMPTY_UTF8
    }

    if (count > 0) {
      subStringIndex(str.asInstanceOf[BinaryStringData], delim.asInstanceOf[BinaryStringData], count)
    }else {
      // 先这样吧, 逻辑简单, 负索引用的也不多
      BinaryStringDataUtil.reverse(subStringIndex(
        BinaryStringDataUtil.reverse(str.asInstanceOf[BinaryStringData]),
        BinaryStringDataUtil.reverse(delim.asInstanceOf[BinaryStringData]),
        -count))
    }
  }

  def subStringIndex(str: BinaryStringData, delim: BinaryStringData, cnt: Int): BinaryStringData = {
    var idx = -1
    var count = cnt
    while (count > 0) {
      idx = str.indexOf(delim, idx + 1)
      if (idx >= 0) {
        count -= 1
      } else {
        // 找不到delim
        return str
      }
    }

    if (idx == 0) BinaryStringData.EMPTY_UTF8 else str.substring(0, idx)
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(3)

  override def stringArgs: Seq[String] = Seq("str", "delim", "count")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] =
    Seq(stringDateType, stringDateType, intDateType)

  override def inferOutputTypes(args: Seq[DataType], callContext: CallContext): DataType = stringDateType
}
