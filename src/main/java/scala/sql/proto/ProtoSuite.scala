package scala.sql.proto

import org.scalatest.funsuite.AnyFunSuite

class ProtoSuite extends AnyFunSuite{

    test("a"){
        val data = LogData.newBuilder()
                .setDt("1").setItemId(1)
                .build()
        val bytes = data.toByteArray
        println(bytes.toList)

        val obj = LogData.parseFrom(bytes)
        println(obj.toString)
    }


}
