package scala.sql.udf.internal

import java.util
import java.util.Optional

import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.{FunctionDefinition, SpecializedFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.Signature.Argument
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext, InputTypeStrategy, Signature, TypeInference, TypeStrategy}

import scala.collection.JavaConverters._

abstract class InternalTableFunction extends TableFunction[RowData] with SpecializedFunction{
  self =>
  import InternalScalarFunction._
  var funcId = nextFuncId

  override def specialize(context: SpecializedFunction.SpecializedContext): UserDefinedFunction = {
    val clazz = context.getCallContext.getFunctionDefinition.getClass
    val func = clazz.newInstance()
    func.asInstanceOf[InternalTableFunction].funcId = nextFuncId
    func.asInstanceOf[UserDefinedFunction]
  }

  def argumentCount: ArgumentCount

  def stringArgs: Seq[String]

  def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType]

  def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType

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
          funcId = nextFuncId
          val dType: DataType = self.inferOutputType(callContext.getArgumentDataTypes().asScala, callContext, typeFactory)
          val claszz = dataTypeConversionClass(dType)
          Optional.of(dType.bridgedTo(claszz))
        }
      })
      .build()
  }
}
