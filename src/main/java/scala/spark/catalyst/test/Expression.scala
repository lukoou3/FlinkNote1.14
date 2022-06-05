package scala.spark.catalyst.test

import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType

abstract class Expression {
  def foldable: Boolean = false

  def children: Seq[Expression]

  lazy val deterministic: Boolean = children.forall(_.deterministic)

  def nullable: Boolean

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: Any): Any

  def checkInputDataTypes(): Boolean = true
  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and input data types checking passed, and `false` if it still contains any unresolved
   * placeholders or has data types mismatch.
   * Implementations of expressions should override this if the resolution of this type of
   * expression involves more than just the resolution of its children and type checking.
   */
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes()

  /**
   * 返回DataType。非法的在unresolved expression上查询dataType
   * Returns the [[com.oracle.jrockit.jfr.DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)
}

/**
 * A leaf expression, i.e. one without any child expressions.
 */
abstract class LeafExpression extends Expression {

  override final def children: Seq[Expression] = Nil
}

/**
 * 两个输入，一个输出。默认情况下，任何一个参数为null，则输出为null
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class BinaryExpression extends Expression {

  def left: Expression
  def right: Expression

  override final def children: Seq[Expression] = Seq(left, right)

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: Any): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

}

/**
 * 当表达式继承此项时，意味着表达式是null过敏的（即，任何null输入都将导致null输出）。我们将在构造IsNotNull约束时使用此信息。
 * When an expression inherits this, meaning the expression is null intolerant (i.e. any null
 * input will result in null output). We will use this information during constructing IsNotNull
 * constraints.
 */
trait NullIntolerant extends Expression

trait ExpectsInputTypes extends Expression