package scala.util

import java.beans.{Introspector, PropertyDescriptor}

import org.apache.flink.streaming.api.scala.createTypeInformation

import scala.beans.BeanProperty

object JavaReflection {

  def getJavaBeanReadableProperties[T](beanClass: Class[T]): Array[PropertyDescriptor] = {
    val beanInfo = Introspector.getBeanInfo(beanClass)
    beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
      .filterNot(_.getName == "declaringClass")
      .filter(_.getReadMethod != null)
  }

  def getJavaBeanReadableAndWritableProperties[T](
                                                   beanClass: Class[T]): Array[PropertyDescriptor] = {
    getJavaBeanReadableProperties(beanClass).filter(_.getWriteMethod != null)
  }

  def main(args: Array[String]): Unit = {
    val propertyDescriptors = JavaReflection.getJavaBeanReadableAndWritableProperties(classOf[Data])
    propertyDescriptors.foreach(println(_))

    val typeInformation = createTypeInformation[Data]
    println(typeInformation)

    val typeInformation2 = createTypeInformation[ScalaBean]
    println(typeInformation2)
  }

  class Data{
    @BeanProperty
    var id: Int = _
    @BeanProperty
    var name: String = _
    @BeanProperty
    var age: Integer = _
  }

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
}
