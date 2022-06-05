package scala.sql.udf.internal

import java.util
import java.util.{Collections, Optional}

import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{ArrayData, MapData, RowData, StringData, TimestampData}
import org.apache.flink.table.functions.{FunctionDefinition, ScalarFunction}
import org.apache.flink.table.types.inference.Signature.Argument
import org.apache.flink.table.types.{AtomicDataType, DataType}
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext, ConstantArgumentCount, InputTypeStrategy, Signature, TypeInference, TypeStrategy}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{IntType, LogicalType, LogicalTypeRoot, RowType, VarCharType}

import scala.collection.JavaConverters._
import scala.math.Ordering

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
  def betweenArgumentCount(minCount: Int, maxCount: Int) = ConstantArgumentCount.between(minCount,maxCount )

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

  def getOrdering(logicalType: LogicalType):Ordering[Any] = {
    (logicalType.getTypeRoot match {
      case INTEGER => implicitly[Ordering[Int]]
      case BIGINT => implicitly[Ordering[Long]]
      case FLOAT => implicitly[Ordering[Float]]
      case DOUBLE => implicitly[Ordering[Double]]
      case CHAR | VARCHAR => new Ordering[StringData]{
        override def compare(x: StringData, y: StringData): Int = x.compareTo(y)
      }
      case ROW => new Ordering[RowData] {
        val orderings = logicalType.asInstanceOf[RowType].getFields.asScala.map{field =>
          getOrdering(field.getType)
        }.toArray
        val fieldGetters = logicalType.asInstanceOf[RowType].getFields.asScala.zipWithIndex.map{ case(field, i) =>
          RowData.createFieldGetter(field.getType, i)
        }.toArray
        override def compare(x: RowData, y: RowData): Int = {
          var i = 0
          val size = orderings.size
          while (i < size) {
            val order = orderings(i)
            val left = fieldGetters(i).getFieldOrNull(x)
            val right = fieldGetters(i).getFieldOrNull(y)

            if (left == null && right == null) {
              // Both null, continue looking.
            } else if (left == null) {
              return -1
            } else if (right == null) {
              return 1
            } else {
              val comparison = order.compare(left, right)
              if (comparison != 0) {
                return comparison
              }
            }

            i += 1
          }

          0
        }
      }
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
    }).asInstanceOf[Ordering[Any]]
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
      case BOOLEAN => classOf[java.lang.Boolean]
      case ROW => classOf[RowData]
      case ARRAY => classOf[ArrayData]
      case MAP => classOf[MapData]
      case MULTISET => classOf[MapData]
      case TIME_WITHOUT_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE => classOf[TimestampData]
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${dType.getLogicalType.getTypeRoot}")
    }
  }
}
