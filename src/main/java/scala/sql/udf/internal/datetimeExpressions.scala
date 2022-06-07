package scala.sql.udf.internal

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import InternalScalarFunction._

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

