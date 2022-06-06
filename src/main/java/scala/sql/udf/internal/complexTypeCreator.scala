package scala.sql.udf.internal

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}

import InternalScalarFunction._
import scala.annotation.varargs

class CreateNamedStruct extends InternalScalarFunction{
  var size = 0

  @varargs
  def eval(elems: AnyRef*): RowData = {
    val row = new GenericRowData(size)
    var i = 0
    while(2*i + 1 < elems.length){
      row.setField(i, elems(2*i + 1))
      i += 1
    }
    row
  }

  override def argumentCount: ArgumentCount = anyArgumentCount

  override def stringArgs: Seq[String] = Seq("name1", "val1", "name2", "val2", "...")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = args

  override def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    if(args.isEmpty || args.length % 2 == 1){
      throw callContext.newValidationError("参数个数必须是偶数")
    }

    if(!(0 until args.length by 2).forall(callContext.isArgumentLiteral(_))){
      throw callContext.newValidationError("奇数参数类型必须是字面量")
    }

    size = args.length / 2

    val names = (0 until args.length by 2).map(callContext.getArgumentValue(_, classOf[String]).get())
    val values = args.grouped(2).map {case Seq(name, value) => value}.toSeq
    val fields = names.zip(values).map{ case (name, value) => DataTypes.FIELD(name, value)}

    if(names.toSet.size != names.size){
      throw callContext.newValidationError("属性参数不能重复")
    }

    DataTypes.ROW(fields:_*)
  }
}
