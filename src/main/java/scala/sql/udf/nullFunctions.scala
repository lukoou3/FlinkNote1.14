package scala.sql.udf

import java.util.Optional

import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{CallContext, TypeInference, TypeStrategy}
import org.apache.flink.table.types.logical.LogicalTypeRoot

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
          if(argumentDataTypes.toSet.size > 1){
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
