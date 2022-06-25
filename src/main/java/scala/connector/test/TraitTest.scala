package scala.connector.test

object TraitTest {

  def main(args: Array[String]): Unit = {
    val obj = new ChildClass
    obj.print()
    println(obj.get())

    println()
    println("#" * 50)
    println()

    val obj2 = new ChildClass2
    obj2.print()
    println(obj2.get())

    println()
    println("#" * 50)
    println()

    val obj3 = new ChildClass3
    obj3.print()
    println(obj3.get())

    println()
    println("#" * 50)
    println()

    val obj4 = new ChildClass4
    obj4.print()
    println(obj4.get())
  }

  class ChildClass extends Child1Trait with Child2Trait{
    override def print(): Unit = {
      println("ChildClass")
      super.print()
    }

    override def get(): String = {
      println("super.get()", super.get())
      "ChildClass"
    }
  }

  class ChildClass2 extends Child1Trait with Child2Trait{
  }

  class Child1Class extends ParentTrait {
    override def print(): Unit = {
      println("Child1Class")
      super.print()
    }

    override def get(): String = {
      super.get()
      "Child1Class"
    }
  }

  class ChildClass3 extends Child1Class with Child1Trait with Child2Trait{
    override def print(): Unit = {
      println("ChildClass3")
      super.print()
    }

    override def get(): String = {
      println("super.get()", super.get())
      "ChildClass3"
    }
  }

  class ChildClass4 extends ChildClass with Child1Trait with Child2Trait{

  }

  trait ParentTrait{
    def print() : Unit = println("ParentTrait")
     def get() : String = "ParentTrait"
  }

  trait Child1Trait extends ParentTrait{
    override def print() : Unit = {
      println("Child1Trait")
      super.print()
    }
    override def get() : String = "Child1Trait"
  }

  trait Child2Trait extends ParentTrait{
    override def print() : Unit = {
      println("Child2Trait")
      super.print()
    }
    override def get() : String = "Child2Trait"
  }


}
