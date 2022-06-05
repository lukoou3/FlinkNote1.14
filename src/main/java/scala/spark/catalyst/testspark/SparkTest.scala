package scala.spark.catalyst.testspark

import org.apache.spark.sql.catalyst.analysis.ResolveLambdaVariables2
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, NamedLambdaVariable}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite

class SparkTest extends AnyFunSuite {

  test("aa"){
    import org.apache.spark.sql.catalyst.expressions.EmptyRow
    val conf = new SQLConf
    val parser = new SparkSqlParser(conf)
    val expression: Expression = parser.parseExpression("x -> x >= 3 and x <= 5")
    println(expression.getClass)
    println(expression)

    val resolveLambdaVariables:ResolveLambdaVariables2 = ResolveLambdaVariables2(conf)

    val lambdaFunction:LambdaFunction = resolveLambdaVariables.createLambda(expression, Seq((IntegerType, false)))

    /**
     * 不要解析未绑定的lambda函数。
     * 如果我们看到这样一个lambda函数，这意味着要么高阶函数尚未解析，要么我们看到的是悬而未决的lambda函数。
     */
    //val expression1: Expression = ResolveLambdaVariables.resolve(expression, Map.empty)
    val expression1: Expression = resolveLambdaVariables.resolve(lambdaFunction, Map.empty)
    println(expression1)
    val value = lambdaFunction.arguments.head.asInstanceOf[NamedLambdaVariable]
    value.value.set(1)
    println(expression1.eval(EmptyRow))// new GenericInternalRow(Array[Any](1))
    value.value.set(4)
    println(expression1.eval(EmptyRow))
  }

}
