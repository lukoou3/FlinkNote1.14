package scala.spark.catalyst.test

import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import scala.math.Numeric

abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes{
  def symbol: String

  def sqlOperator: String = symbol

  override def toString: String = s"($left $sqlOperator $right)"
}

abstract class BinaryArithmetic extends BinaryOperator with NullIntolerant {
  override def dataType: DataType = left.dataType
}

case class Add(
  left: Expression,
  right: Expression) extends BinaryArithmetic {

  override def symbol: String = "+"

  private lazy val numeric: Numeric[Any] = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any =  {
    numeric.plus(input1, input2)
  }

}

case class Subtract(
  left: Expression,
  right: Expression) extends BinaryArithmetic {

  override def symbol: String = "-"

  private lazy val numeric: Numeric[Any] = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any =  {
    numeric.minus(input1, input2)
  }

}

object TypeUtils{
  def getNumeric(dataType: DataType): Numeric[Any] = {
    (dataType.getLogicalType.getTypeRoot match {
      case INTEGER => implicitly[Numeric[Int]]
      case BIGINT => implicitly[Numeric[Long]]
      case FLOAT => implicitly[Numeric[Float]]
      case DOUBLE => implicitly[Numeric[Double]]
    }).asInstanceOf[Numeric[Any]]
  }
}