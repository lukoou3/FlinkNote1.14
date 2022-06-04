package scala.sql.udf

import java.util
import java.util.{Collections, Optional}

import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.StringData
import org.apache.flink.table.functions.{FunctionContext, FunctionDefinition, ScalarFunction}
import org.apache.flink.table.types.{AtomicDataType, DataType}
import org.apache.flink.table.types.inference.Signature.Argument
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext, ConstantArgumentCount, InputTypeStrategy, Signature, TypeInference, TypeStrategy}
import org.apache.flink.table.types.logical.{LogicalTypeRoot, VarCharType}
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import scala.collection.JavaConverters._

class Nvl extends ScalarFunction{
  var dataType: DataType = _

  override def open(context: FunctionContext): Unit = {
    assert(dataType != null)
  }

  def eval(left: AnyRef, right: AnyRef): AnyRef = {
    if (left != null) left else right
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes.asScala
          // not null
          if(argumentDataTypes.map(_.getLogicalType.getTypeRoot).toSet.size > 1){
            throw callContext.newValidationError("must is same type.")
          }

          val dType: DataType = argumentDataTypes(0)
          dataType = dType
          assert(dataType != null && dataType.getLogicalType.getTypeRoot != LogicalTypeRoot.NULL)

          Optional.of(dType)
        }
      })
      .build()
  }

}

class NvlInternal extends ScalarFunction{
  var dataType: DataType = _

  override def open(context: FunctionContext): Unit = {
    assert(dataType != null)
  }

  def eval(left: AnyRef, right: AnyRef): AnyRef = {
    if (left != null) left else right
  }


  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .inputTypeStrategy(
        new InputTypeStrategy{
          override def getArgumentCount: ArgumentCount = ConstantArgumentCount.of(2)

          override def inferInputTypes(callContext: CallContext, throwOnFailure: Boolean): Optional[util.List[DataType]] = {
            val args: util.List[DataType] = callContext.getArgumentDataTypes()
            val arg0: DataType = args.get(0).nullable()
            val arg1: DataType = args.get(1).nullable()

            val claszz = dataTypeConversionClass(arg0)
            Optional.of(util.Arrays.asList(arg0.bridgedTo(claszz), arg1.bridgedTo(claszz)))
          }

          override def getExpectedSignatures(definition: FunctionDefinition): util.List[Signature] = {
            Collections.singletonList(Signature.of(Argument.of("left"), Argument.of("right")))
          }
        }
      )
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes.asScala
          if(argumentDataTypes.map(_.nullable()).toSet.size > 1){
            throw callContext.newValidationError("must is same type.")
          }

          val dType: DataType = argumentDataTypes(0).nullable()
          dataType = dType
          assert(dataType != null && dataType.getLogicalType.getTypeRoot != LogicalTypeRoot.NULL)

          val claszz = dataTypeConversionClass(dType)
          Optional.of(dType.bridgedTo(claszz))
        }
      })
      .build()
  }

  def dataTypeConversionClass(dType: DataType): Class[_] ={
    dType.getLogicalType.getTypeRoot match {
      case CHAR | VARCHAR => classOf[StringData]
      case INTEGER => classOf[Integer]
      case BIGINT => classOf[java.lang.Long]
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${dType.getLogicalType.getTypeRoot}")
    }
  }
}

class NvlInternal2 extends ScalarFunction{
  var dataType: DataType = _

  override def open(context: FunctionContext): Unit = {
    assert(dataType != null)
  }

  def eval(left: AnyRef, right: AnyRef): AnyRef = {
    if (left != null) left else right
  }


  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .inputTypeStrategy(
        new InputTypeStrategy{
          override def getArgumentCount: ArgumentCount = ConstantArgumentCount.of(2)

          override def inferInputTypes(callContext: CallContext, throwOnFailure: Boolean): Optional[util.List[DataType]] = {
            val args: util.List[DataType] = callContext.getArgumentDataTypes()
            val arg0: DataType = args.get(0).nullable()

            val claszz = dataTypeConversionClass(arg0)
            Optional.of(util.Arrays.asList(arg0.bridgedTo(claszz), arg0.bridgedTo(claszz)))
          }

          override def getExpectedSignatures(definition: FunctionDefinition): util.List[Signature] = {
            Collections.singletonList(Signature.of(Argument.of("left"), Argument.of("right")))
          }
        }
      )
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes.asScala
          /*if(argumentDataTypes.map(_.nullable()).toSet.size > 1){
            throw callContext.newValidationError("must is same type.")
          }*/

          val dType: DataType = argumentDataTypes(0).nullable()
          dataType = dType
          assert(dataType != null && dataType.getLogicalType.getTypeRoot != LogicalTypeRoot.NULL)

          val claszz = dataTypeConversionClass(dType)
          Optional.of(dType.bridgedTo(claszz))
        }
      })
      .build()
  }

  def dataTypeConversionClass(dType: DataType): Class[_] ={
    dType.getLogicalType.getTypeRoot match {
      case CHAR | VARCHAR => classOf[StringData]
      case INTEGER => classOf[Integer]
      case BIGINT => classOf[java.lang.Long]
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${dType.getLogicalType.getTypeRoot}")
    }
  }
}

// InternalScalarFunction
class NvlInternal3 extends ScalarFunction{
  var dataType: DataType = _

  override def open(context: FunctionContext): Unit = {
    assert(dataType != null)
  }

  def eval(left: AnyRef, right: AnyRef): AnyRef = {
    if (left != null) left else right
  }


  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .inputTypeStrategy(
        new InputTypeStrategy{
          override def getArgumentCount: ArgumentCount = ConstantArgumentCount.of(2)

          override def inferInputTypes(callContext: CallContext, throwOnFailure: Boolean): Optional[util.List[DataType]] = {
            val args: util.List[DataType] = callContext.getArgumentDataTypes()
            val optionDataType = findTightestCommonType(args.get(0), args.get(1))
            val dataType = optionDataType.getOrElse(throw new Exception("must same type"))
            val claszz = dataTypeConversionClass(dataType)
            Optional.of(util.Arrays.asList(dataType.bridgedTo(claszz), dataType.bridgedTo(claszz)))
          }

          override def getExpectedSignatures(definition: FunctionDefinition): util.List[Signature] = {
            Collections.singletonList(Signature.of(Argument.of("left"), Argument.of("right")))
          }
        }
      )
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes.asScala
          /*if(argumentDataTypes.map(_.nullable()).toSet.size > 1){
            throw callContext.newValidationError("must is same type.")
          }*/

          val dType: DataType = argumentDataTypes(0).nullable()
          dataType = dType
          assert(dataType != null && dataType.getLogicalType.getTypeRoot != LogicalTypeRoot.NULL)

          val claszz = dataTypeConversionClass(dType)
          Optional.of(dType.bridgedTo(claszz))
        }
      })
      .build()
  }

  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (t1, t2) if t1.getLogicalType.getTypeRoot == t2.getLogicalType.getTypeRoot =>
      if(t1.getLogicalType.isNullable )  Some(t1) else Some(t2)
    case (t1, t2) if t1.getLogicalType.getTypeRoot == BIGINT && Set(INTEGER).contains(t2.getLogicalType.getTypeRoot) =>
      if(t1.getLogicalType.isNullable || t2.getLogicalType.isNullable)  Some(t1) else Some(t1)
    case (t2, t1) if t1.getLogicalType.getTypeRoot == BIGINT && Set(INTEGER).contains(t2.getLogicalType.getTypeRoot) =>
      if(t1.getLogicalType.isNullable || t2.getLogicalType.isNullable)  Some(t1) else Some(t1)
    case (t1, t2) if Seq(t1, t2).map(_.getLogicalType.getTypeRoot).forall(x =>  x == CHAR || x == VARCHAR) =>
      Some(new AtomicDataType(new VarCharType(VarCharType.MAX_LENGTH)))
    case _ => None
  }

  def dataTypeConversionClass(dType: DataType): Class[_] ={
    dType.getLogicalType.getTypeRoot match {
      case CHAR | VARCHAR => classOf[StringData]
      case INTEGER => classOf[Integer]
      case BIGINT => classOf[java.lang.Long]
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${dType.getLogicalType.getTypeRoot}")
    }
  }
}