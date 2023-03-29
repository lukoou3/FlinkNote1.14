package scala.sql.proto

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import scala.util.TsUtils

class ProtoFormatSuite extends AnyFunSuite with BeforeAndAfterAll{
    var env: StreamExecutionEnvironment = _
    var tEnv: StreamTableEnvironment = _

    override protected def beforeAll(): Unit = {
        val conf = new Configuration()
        conf.setString("rest.bind-port", "8081-8085")
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
        env.setParallelism(1)
        env.getConfig.enableObjectReuse()

        val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
        tEnv = StreamTableEnvironment.create(env, settings)
    }

    def getSource() =new RichParallelSourceFunction[Array[Byte]] {
        var stop = false
        override def run(ctx: SourceFunction.SourceContext[Array[Byte]]): Unit = {
            var i = 0
            val time0 = TsUtils.timestamp(System.currentTimeMillis()).toString
            //val str = "a" * 1000
            val str = "a"
            while (!stop) {
                val dt = TsUtils.daysFrom1970(i / 1000)
                val bs = "hotel" + i
                val time = time0
                val browser_uniq_id = "browser_uniq_id" + i
                val os_plant = "ANDROID" + i
                //val page_id = page_ids(Random.nextInt(page_ids.length))
                val page_id = "HotelRN_Detail" + i
                val page_param = s"1_43403_56400_${dt}_${dt}_天津金皇大酒店_" + i
                //val data = str + i
                val data = LogData.newBuilder()
                        .setDt(dt + "").setBs(bs).setReportTime(s"${time}.${i}")
                        .setBrowserUniqId(browser_uniq_id).setOsPlant(os_plant).setPageId(page_id)
                        .setPageName("HotelRN_Detail_Activity").setPageParam(page_param)
                        .setItemId(i).setItemType(0).setVisitTime(time)
                        .build()
                val bytes = data.toByteArray
                ctx.collect(bytes)
                ctx.collect(bytes)
                ctx.collect(bytes)
                ctx.collect(bytes)
                i += 1
            }
        }

        override def cancel(): Unit = this.stop = true
    }

    // 3.5之前会直接丢弃未知字段，读取少量字段时更加高效
    test("proto_des"){
        val ds = env.addSource(getSource()).disableChaining()
        ds.map(new RichMapFunction[Array[Byte], LogData] {
            override def map(bytes: Array[Byte]): LogData = {
                LogData.parseFrom(bytes)
            }
        }).addSink(new RichSinkFunction[LogData] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: LogData, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    println(value.getPageParam)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("proto_des2"){
        val ds = env.addSource(getSource()).disableChaining()
        ds.map(new RichMapFunction[Array[Byte], LogData2] {
            override def map(bytes: Array[Byte]): LogData2 = {
                LogData2.parseFrom(bytes)
            }
        }).addSink(new RichSinkFunction[LogData2] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: LogData2, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    value.toString()
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    override protected def afterAll(): Unit = {
        env.execute()
    }
}
