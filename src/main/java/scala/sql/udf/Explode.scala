package scala.sql.udf

import java.util.Optional

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.types.inference.{CallContext, TypeInference, TypeStrategy}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.{CollectionDataType, DataType}
import org.apache.flink.types.Row


class Explode extends TableFunction[Row]{

  def eval(eles: Array[AnyRef]): Unit ={
    if(eles != null){
      eles.foreach{ ele =>
        collect(Row.of(ele))
      }
    }
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes
          if(argumentDataTypes.size() != 1 || argumentDataTypes.get(0).getLogicalType.getTypeRoot != ARRAY){
            throw callContext.newValidationError(s"input to function explode should be array or map type, not $argumentDataTypes")
          }

          val dType = argumentDataTypes.get(0).asInstanceOf[CollectionDataType].getElementDataType
          Optional.of(DataTypes.ROW(DataTypes.FIELD("col", dType)))
        }
      })
      .build()
  }
}

class PosExplode extends TableFunction[Row]{

  def eval(eles: Array[AnyRef]): Unit ={
    if(eles != null){
      eles.zipWithIndex.foreach{ case (ele, i) =>
        // 这编译器还是不够智能啊，int不能直接传入obj
        collect(Row.of(i:Integer, ele))
      }
    }
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      .outputTypeStrategy(new TypeStrategy {
        override def inferType(callContext: CallContext): Optional[DataType] = {
          val argumentDataTypes = callContext.getArgumentDataTypes
          if(argumentDataTypes.size() != 1 || argumentDataTypes.get(0).getLogicalType.getTypeRoot != ARRAY){
            throw callContext.newValidationError(s"input to function explode should be array or map type, not $argumentDataTypes")
          }

          val dType = argumentDataTypes.get(0).asInstanceOf[CollectionDataType].getElementDataType
          Optional.of(DataTypes.ROW(DataTypes.FIELD("pos", DataTypes.INT()), DataTypes.FIELD("col", dType)))
        }
      })
      .build()
  }

}