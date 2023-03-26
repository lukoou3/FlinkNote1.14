package scala.connector.test

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.util.{Random, TsUtils}

object ProducerMsg {

    def main(args: Array[String]): Unit = {
        val props = new Properties()
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
        props.put("acks", "1")
        // 消息发送最大尝试次数
        props.put("retries", 0+"")
        // 一批消息处理大小
        props.put("batch.size", 16384+"")
        // 请求延时
        props.put("linger.ms", 1+"")
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432+"")
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
        var i = 0
        val page_ids = Array("HotelRN_Detail", "ThemePark_Scenic", "MHotel_Detail")
        try {
            while (true) {
                i += 1
                val dt = TsUtils.daysFrom1970(i / 1000)
                val bs = "hotel" + i
                val time = TsUtils.timestamp(System.currentTimeMillis()).toString
                val browser_uniq_id = "browser_uniq_id" + i
                val os_plant = "ANDROID" + i
                //val page_id = page_ids(Random.nextInt(page_ids.length))
                val page_id = "HotelRN_Detail" + i
                val page_param = s"1_43403_56400_${dt}_${dt}_天津金皇大酒店_" + i
                val json = s"""{"dt": "$dt", "bs": "$bs", "report_time": "$time", "browser_uniq_id": "$browser_uniq_id", "os_plant": "$os_plant", "page_id": "$page_id", "page_name": "HotelRN_Detail_Activity", "page_param": "$page_param", "item_id": ${Random.nextInt(1000000)}, "item_type": 0, "visit_time": "$time"}\n"""
                producer.send(new ProducerRecord[String, String]("app_pv", json))
                producer.send(new ProducerRecord[String, String]("app_pv", json))
                if(i % 1000000 == 0){
                    println(TsUtils.timestamp(System.currentTimeMillis()))
                    println(json)
                }
            }
        } catch {
            case e:Exception =>
        }


        //关闭资源

    }

}
