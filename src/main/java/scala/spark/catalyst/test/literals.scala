package scala.spark.catalyst.test

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical._

object Literal {
  val TrueLiteral: Literal = Literal(true, DataTypes.BOOLEAN())

  val FalseLiteral: Literal = Literal(false, DataTypes.BOOLEAN())

  def apply(v: Any): Literal = v match {
    case i: Int => Literal(i, DataTypes.INT())
    case l: Long => Literal(l, DataTypes.BIGINT())
    case d: Double => Literal(d, DataTypes.DOUBLE())
    case f: Float => Literal(f, DataTypes.FLOAT())
  }
}

/**
 * 字面量类型, 叶子节点, eval方法直接返回value
 * 要做类型检查, 使用Literal.create()代替使用构造器
 * In order to do type checking, use Literal.create() instead of constructor
 */
case class Literal(value: Any, dataType: DataType) extends LeafExpression {
  override def foldable: Boolean = true
  override def nullable: Boolean = value == null

  override def eval(input: Any): Any = value
}