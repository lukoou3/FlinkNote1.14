package scala.connector.test

import java.net.ServerSocket
import org.scalatest.funsuite.AnyFunSuite
import scala.util.Random
import scala.util.TsUtils

class SocketWriteSuite extends AnyFunSuite {

    test("1"){
        val server = new ServerSocket(9000)
        while(true){
            val socket = server.accept()
            val out = socket.getOutputStream()
            var i = 0
            val page_ids = Array("HotelRN_Detail", "ThemePark_Scenic", "MHotel_Detail")

            try {
                while (true) {
                    val dt = TsUtils.daysFrom1970(i / 1000)
                    val bs = "hotel" + i
                    val time = TsUtils.timestamp(System.currentTimeMillis()).toString
                    val browser_uniq_id = "browser_uniq_id" + i
                    val os_plant = "ANDROID" + i
                    //val page_id = page_ids(Random.nextInt(page_ids.length))
                    val page_id = "HotelRN_Detail" + i
                    val page_param = s"1_43403_56400_${dt}_${dt}_天津金皇大酒店_" + i
                    val json = s"""{"dt": "$dt", "bs": "$bs", "report_time": "$time", "browser_uniq_id": "$browser_uniq_id", "os_plant": "$os_plant", "page_id": "$page_id", "page_name": "HotelRN_Detail_Activity", "page_param": "$page_param", "item_id": ${Random.nextInt(page_ids.length)}, "item_type": 0, "visit_time": "$time"}\n"""
                    val bytes = json.getBytes("utf-8")
                    out.write(bytes)
                    //out.write(bytes)

                    i += 1
                    if (i % 10 == 0) {
                        out.flush()
                    }
                    if(i % 1000000 == 0){
                        println(TsUtils.timestamp(System.currentTimeMillis()))
                        println(json)
                    }
                }
            } catch {
                case e:Exception =>
            }
        }

    }

}
