package scala.spark.catalyst.test

package object dsl {
  trait ImplicitOperators {
    def expr: Expression

    def + (other: Expression): Expression = Add(expr, other)

    def - (other: Expression): Expression = Subtract(expr, other)
  }

  trait ExpressionConversions {
    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr: Expression = e
    }

    implicit def booleanToLiteral(b: Boolean): Literal = Literal(b)
    implicit def shortToLiteral(s: Short): Literal = Literal(s)
    implicit def intToLiteral(i: Int): Literal = Literal(i)
    implicit def longToLiteral(l: Long): Literal = Literal(l)
    implicit def floatToLiteral(f: Float): Literal = Literal(f)
    implicit def doubleToLiteral(d: Double): Literal = Literal(d)
  }

  object expressions extends ExpressionConversions
}
