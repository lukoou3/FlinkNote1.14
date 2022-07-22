package scala.stream.typeinfo

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala._
import org.scalatest.funsuite.AnyFunSuite

import scala.beans.BeanProperty
import TypeInformationSuite._

class TypeInformationSuite extends AnyFunSuite {
  /**
   * [[org.apache.flink.streaming.api.scala.createTypeInformation]]
   * [[org.apache.flink.api.scala.typeutils.TypeUtils.createTypeInfo]]
   * [[org.apache.flink.api.scala.codegen.TypeInformationGen.mkTypeInfo]]
   * [org.apache.flink.api.scala.codegen.TypeAnalyzer.UDTAnalyzerInstance#analyze]
   * 能解析出OptionType, CaseClassType, JavaTupleType, JavaType...。其他的类型都当做pojo解析
   */
  def getTypeInformation[R: TypeInformation](): TypeInformation[R] ={
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    outType
  }

  test("pojoScalaBean"){
    val pojoTypeInformation: TypeInformation[ScalaBean2] = getTypeInformation[ScalaBean2]()
    println(pojoTypeInformation.getClass)
    println(pojoTypeInformation)
  }

  test("ScalaCase"){
    val caseClassTypeInformation: TypeInformation[ScalaCase] = getTypeInformation[ScalaCase]()
    println(caseClassTypeInformation.getClass)
    println(caseClassTypeInformation.isInstanceOf[CaseClassTypeInfo[_]])
    println(caseClassTypeInformation)
  }

}

object TypeInformationSuite{
  // 错误!!!, 属性写到构造函数里了
  class ScalaBean(
    @BeanProperty
    var code: Int = 0,
    @BeanProperty
    var name: String = null,
    @BeanProperty
    var age: Int = 0,
    @BeanProperty
    var birthday: String = null
  ) extends Serializable

  class ScalaBean2 extends Serializable{
    @BeanProperty
    var code: Int = 0
    @BeanProperty
    var name: String = null
    @BeanProperty
    var age: Int = 0
    @BeanProperty
    var birthday: String = null
  }

  case class ScalaCase(
    var code: Int = 0,
    var name: String = null,
    var age: Int = 0,
    var birthday: String = null
  )
}