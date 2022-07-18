package scala.sql.udf.internal

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{StringData, TimestampData}
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import InternalScalarFunction._

/**
 * 时间函数的处理可以看下这两个类，spark相对来说处理种类较多，而且尽量使用sql内置类型，性能相对较好
 * [org.apache.flink.table.runtime.functions.SqlDateTimeUtils]
 * [org.apache.spark.sql.catalyst.util.DateTimeUtils#stringToDate]
 * flink sql函数的解析主要看：
 * [[org.apache.flink.table.planner.codegen.calls.StringCallGen]]
 * [[org.apache.flink.table.planner.codegen.calls.BuiltInMethods]]
 */
class DateAdd extends InternalScalarFunction{
  var foldable = false

  def eval(dateStr: StringData, days: Integer): Integer = {
    val date = if(dateStr == null) null else SqlDateTimeUtils.dateStringToUnixDate(dateStr.toString)
    eval(date, days)
  }

  def eval(date: Integer, days: Integer): Integer = {
    if(date == null || days == null){
      null
    }else{
      date + days
    }
  }

  // 这个默认就是true，输入确定，输出也是确定的
  // override def isDeterministic: Boolean = true

  def argumentCount: ArgumentCount = fixArgumentCount(2)

  def stringArgs: Seq[String] = Seq("start_date", "num_days")

  def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    args.zipWithIndex.map{ case(tpe, i) =>
      if(i==0){
        if(tpe.getLogicalType.getTypeRoot == DATE){
          tpe
        }else{
          stringDateType
        }
      }else{
        intDateType
      }
    }
  }

  def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    foldable = (0 until args.length).forall(x => callContext.isArgumentLiteral(x))

    DataTypes.DATE()
  }
}

// extract_unix_timestamp
class ExtractUnixTimestamp extends InternalScalarFunction{

  def eval(timestamp: TimestampData): java.lang.Long = {
    if(timestamp == null){
      return null
    }
    timestamp.getMillisecond / 1000L
  }

  def argumentCount: ArgumentCount = fixArgumentCount(1)

  def stringArgs: Seq[String] = Seq("timestamp/timestamp_ltz")

  def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    val typeRoot = args(0).getLogicalType.getTypeRoot
    assert(typeRoot == TIMESTAMP_WITHOUT_TIME_ZONE || typeRoot == TIMESTAMP_WITH_TIME_ZONE)
    args
  }

  def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = DataTypes.BIGINT()
}

