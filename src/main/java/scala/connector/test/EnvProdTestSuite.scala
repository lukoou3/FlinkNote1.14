package scala.connector.test

import org.scalatest.funsuite.AnyFunSuite

import EnvProdTestSuite.Constant

class EnvProdTestSuite extends AnyFunSuite{
  test("a"){
    //import EnvProdTestSuite.PROD_ENV
    val name = Constant.NAME
    println(name)
  }

  test("b"){
    import EnvProdTestSuite.PROD_ENV
    val name = Constant.NAME
    println(name)
  }

  test("c"){
    implicit val PROD_ENV = if(false) EnvProdTestSuite.PROD_ENV else EnvProdTestSuite.TEST_ENV
    val name = Constant.NAME
    println(name)
  }
}

object EnvProdTestSuite {
  implicit val PROD_ENV:Boolean = true
  implicit val TEST_ENV:Boolean = false
  object Constant {
    def NAME(implicit prod: Boolean = true): String = if(prod) "prod_name" else "test_name"
  }
}