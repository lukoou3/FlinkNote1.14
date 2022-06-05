package scala.spark.catalyst.test

import java.util.concurrent.atomic.AtomicReference

import org.apache.flink.table.types.DataType

trait NamedExpression extends Expression {
  def name: String
}

case class NamedLambdaVariable(
  name: String,
  dataType: DataType,
  nullable: Boolean,
  value: AtomicReference[Any] = new AtomicReference())
  extends LeafExpression with NamedExpression{

  override def eval(input: Any): Any = value.get
}

case class LambdaFunction(
  function: Expression,
  arguments: Seq[NamedExpression],
  hidden: Boolean = false)
  extends Expression{

  override def children: Seq[Expression] = function +: arguments
  override def dataType: DataType = function.dataType
  override def nullable: Boolean = function.nullable

  override def eval(input: Any): Any = {
    function.eval(input)
  }

}
