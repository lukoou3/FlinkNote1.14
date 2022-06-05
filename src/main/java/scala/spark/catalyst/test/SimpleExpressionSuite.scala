package scala.spark.catalyst.test

import org.apache.flink.table.api.DataTypes
import org.scalatest.funsuite.AnyFunSuite

class SimpleExpressionSuite extends AnyFunSuite {

  test("Add"){
    val a = Literal(1)
    val b = Literal(2)

    val addExpression = Add(a, b)
    val rst = addExpression.eval(null)
    println(rst)
  }

  test("Add dsl"){
    import scala.spark.catalyst.test.dsl.expressions._
    val a: Expression = 10
    val b: Expression = 20

    val addExpression = a + b
    val rst = addExpression.eval(null)
    println(rst)
  }

  test("NamedLambdaVariable"){
    import scala.spark.catalyst.test.dsl.expressions._
    val a = NamedLambdaVariable("arg", DataTypes.INT(), false)
    val b: Expression = 20
    val c: Expression = 10

    val addExpression = a + b - c
    for (i <- 0 to 10) {
      a.value.set(i)
      val rst = addExpression.eval(null)
      println(rst)
    }

  }

  test("LambdaFunction"){
    import scala.spark.catalyst.test.dsl.expressions._
    val a = NamedLambdaVariable("arg", DataTypes.INT(), false)
    val b: Expression = 20
    val c: Expression = 10

    val function: Expression = a + b - c
    val arguments: Seq[NamedExpression] = Seq(a)
    val lambdaFunction = LambdaFunction(function, arguments)

    val functionForEval = lambdaFunction.function
    val LambdaFunction(_, (elementVar: NamedLambdaVariable) +: tail, _) = lambdaFunction
    for (i <- 0 to 10) {
      elementVar.value.set(i)
      val rst = functionForEval.eval(null)
      println(rst)
    }

  }

}
