package scala.sql.udf.internal

import java.util
import java.util.{Collections, Optional}

import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{ArrayData, RowData, StringData, TimestampData}
import org.apache.flink.table.functions.{FunctionDefinition, ScalarFunction}
import org.apache.flink.table.types.inference.Signature.Argument
import org.apache.flink.table.types.{AtomicDataType, DataType}
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext, ConstantArgumentCount, InputTypeStrategy, Signature, TypeInference, TypeStrategy}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{IntType, LogicalTypeRoot, VarCharType}

import scala.collection.JavaConverters._

abstract class InternalScalarFunction extends ScalarFunction {
  self =>
  import InternalScalarFunction._
  var funcId = 0

  def argumentCount: ArgumentCount

  def stringArgs: Seq[String]

  def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType]

  def inferOutputTypes(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .inputTypeStrategy(
        new InputTypeStrategy{
          override def getArgumentCount: ArgumentCount = argumentCount

          override def inferInputTypes(callContext: CallContext, throwOnFailure: Boolean): Optional[util.List[DataType]] = {
            val types = self.inferInputTypes(callContext.getArgumentDataTypes().asScala, callContext).map { dataType =>
              val claszz = dataTypeConversionClass(dataType)
              dataType.bridgedTo(claszz)
            }.toBuffer.asJava
            Optional.of(types)
          }

          override def getExpectedSignatures(definition: FunctionDefinition): util.List[Signature] = {
            stringArgs.map(x => Signature.of(Argument.of(x))).asJava
          }
        }
      )
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          funcId += 1
          val dType: DataType = self.inferOutputTypes(callContext.getArgumentDataTypes().asScala, callContext, typeFactory)
          val claszz = dataTypeConversionClass(dType)
          Optional.of(dType.bridgedTo(claszz))
        }
      })
      .build()
  }


}

object InternalScalarFunction{
  def anyArgumentCount = ConstantArgumentCount.any()
  def fixArgumentCount(fix: Int) = ConstantArgumentCount.of(fix)

  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (t1, t2) if t2.getLogicalType.getTypeRoot == NULL => Some(t1)
    case (t2, t1) if t2.getLogicalType.getTypeRoot == NULL => Some(t1)
    case (t1, t2) if t1.getLogicalType.getTypeRoot == t2.getLogicalType.getTypeRoot =>
      if(t1.getLogicalType.isNullable )  Some(t1) else Some(t2)
    case (t1, t2) if t1.getLogicalType.getTypeRoot == BIGINT && Set(INTEGER).contains(t2.getLogicalType.getTypeRoot) =>
      if(t1.getLogicalType.isNullable || t2.getLogicalType.isNullable)  Some(t1) else Some(t1)
    case (t2, t1) if t1.getLogicalType.getTypeRoot == BIGINT && Set(INTEGER).contains(t2.getLogicalType.getTypeRoot) =>
      if(t1.getLogicalType.isNullable || t2.getLogicalType.isNullable)  Some(t1) else Some(t1)
    case (t1, t2) if Seq(t1, t2).map(_.getLogicalType.getTypeRoot).forall(x =>  x == CHAR || x == VARCHAR) =>
      Some(stringDateType)
    case _ => None
  }

  def stringDateType: DataType = new AtomicDataType(new VarCharType(VarCharType.MAX_LENGTH))
  def intDateType: DataType = new AtomicDataType(new IntType)

  def dataTypeConversionClass(dType: DataType): Class[_] ={
    dType.getLogicalType.getTypeRoot match {
      case CHAR | VARCHAR => classOf[StringData]
      case INTEGER => classOf[Integer]
      case BIGINT => classOf[java.lang.Long]
      case FLOAT => classOf[java.lang.Float]
      case DOUBLE => classOf[java.lang.Double]
      case ROW => classOf[RowData]
      case ARRAY => classOf[ArrayData]
      case TIME_WITHOUT_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE => classOf[TimestampData]
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${dType.getLogicalType.getTypeRoot}")
    }
  }
}
