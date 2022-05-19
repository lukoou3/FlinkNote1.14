package scala.sql.conver

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.scalatest.funsuite.AnyFunSuite

import scala.beans.BeanProperty

class DsToTableSuite extends AnyFunSuite{
  import DsToTableSuite._

  /***
   * table解析只支持解析最外面一层的类型，包含bean等不能再解析转换，都1.14了。这点和spark sql差的很远
   */
  test("DsComplexBeanToTable"){
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)


    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)
    println("Source parallelism:" + text.parallelism)

    val ds = text.map(x => {
      val data = new Data
      data.subData = new SubData
      data
    })
    val typeInformation = createTypeInformation[Data]
    println(typeInformation)

    val table = tEnv.fromDataStream(ds)
    table.printSchema()

    //table.toAppendStream[Row].print()
    val rstDs = table.toDataStream
    println(rstDs.dataType)
    rstDs.print()

    env.execute("Window Stream WordCount")
  }

}

object DsToTableSuite{
  class Data {
    @BeanProperty
    var id: Int = _
    @BeanProperty
    var name: String = _
    @BeanProperty
    var age: Integer = _
    @BeanProperty
    var subData: SubData = _

    override def toString = s"Data(id=$id, name=$name, age=$age, subData=$subData)"
  }

  class SubData{
    @BeanProperty
    var id: Int = _
    @BeanProperty
    var name: String = _
    @BeanProperty
    var age: Integer = _
    override def toString = s"SubData(id=$id, name=$name, age=$age)"
  }



}