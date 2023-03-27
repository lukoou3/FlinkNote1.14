package scala.connector.test

import com.alibaba.fastjson.JSON
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
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import scala.beans.BeanProperty
import scala.connector.format.{FastJson2JavaBeanDeserialization, JsonJavaBeanDeserialization, JsonRowDataSimpleDeserializationSchema, JsonRowDataSimpleDeserializationSchemaJava}
import scala.connector.test.JsonFormatSuite.JavaBean
import scala.util.{Random, TsUtils}
import scala.sql.utils.TableImplicits.StreamTableEnvOps
import scala.stream.func.DeserializeFunc

class JsonFormatSuite extends AnyFunSuite with BeforeAndAfterAll {
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
    test("myjsonjava1") {
        /**
         * {"id":"1","name":"罗隐32","age":1300,"text":11}
         * {"id":"1", "name":"罗隐", "age":30,"text":"11"}
         * {"id":"1", "name":"罗隐", "age":30,"text":{"name": "aa"}}
         * {"id":"1", "name":"罗隐","text":{"name": 11  , "age":30}, "age":30}
         * {"id":1,"text":11}
         * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
         */
        var sql = """
    CREATE TABLE tmp_tb1 (
      id int,
      name string,
      age int,
      text string
      -- PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
      'connector' = 'mysocket',
      'hostname' = 'localhost',
      'port' = '9999',
      'format' = 'myjsonjava'
    )
    """
        tEnv.executeSql(sql)

        sql = """
    select
        *
    from tmp_tb1
    """
        tEnv.sqlQuery(sql).execute().print()
    }

    test("myjsonjava") {
        var sql =
            """
    CREATE TABLE tmp_tb(
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
    ) WITH (
      'connector' = 'mylocalfile',
      'path' = 'D:\IdeaWorkspace\FlinkNote\Flink12\test_data\online_log.json',
      'format' = 'myjsonjava',
      'source.sleep' = '100'
    )
    """
        tEnv.executeSql(sql)

        sql = """
    select
        *
    from tmp_tb
    where page_id like '%tel%' and item_id > 982270
    """
        tEnv.sqlQuery(sql).execute().print()
    }

    test("myjson") {
        var sql =
            """
    CREATE TABLE tmp_tb(
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
    ) WITH (
      'connector' = 'mylocalfile',
      'path' = 'D:\IdeaWorkspace\FlinkNote\Flink12\test_data\online_log.json',
      'format' = 'myjson',
      'source.sleep' = '0'
    )
    """
        tEnv.executeSql(sql)

        sql = """
    select
        *
    from tmp_tb
    """
        val table = tEnv.sqlQuery(sql)
        val rowDataDataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
        val rowDataDs = table.toDataStream[RowData](rowDataDataType)
        rowDataDs.addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var start = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                if(i % 1000000 == 0){
                    val s = (System.currentTimeMillis() - start).toDouble / 1000
                    println(i, s, i / s)
                }
            }
        })
    }

    test("json") {
        var sql =
            """
    CREATE TABLE tmp_tb(
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
    ) WITH (
      'connector' = 'mylocalfile',
      'path' = 'D:\IdeaWorkspace\FlinkNote\Flink12\test_data\online_log.json',
      'format' = 'json',
      'source.sleep' = '0'
    )
    """
        tEnv.executeSql(sql)

        sql = """
    select
        *
    from tmp_tb
    """
        val table = tEnv.sqlQuery(sql)
        val rowDataDataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
        val rowDataDs = table.toDataStream[RowData](rowDataDataType)
        rowDataDs.addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var start = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                if(i % 1000000 == 0){
                    val s = (System.currentTimeMillis() - start).toDouble / 1000
                    println(i, s, i / s)
                }
            }
        })
    }
    override protected def afterAll(): Unit = {
        env.execute()
    }


    test("myjson2") {
        var sql =
            """
    CREATE TABLE tmp_tb(
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
    ) WITH (
      'connector' = 'mysocket',
      'hostname' = 'localhost',
      'port' = '9000',
      'format' = 'myjson'
    )
    """
        tEnv.executeSql(sql)

        sql = """
    select
        *
    from tmp_tb
    """
        val table = tEnv.sqlQuery(sql)
        val rowDataDataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
        val rowDataDs = table.toDataStream[RowData](rowDataDataType)
        rowDataDs.addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var start = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                if(i % 1000000 == 0){
                    val s = (System.currentTimeMillis() - start).toDouble / 1000
                    println(i, s, i / s, TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                }
            }
        })
    }

    test("json2") {
        var sql =
            """
    CREATE TABLE tmp_tb(
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
    ) WITH (
      'connector' = 'mysocket',
      'hostname' = 'localhost',
      'port' = '9000',
      'format' = 'json'
    )
    """
        tEnv.executeSql(sql)

        sql = """
    select
        *
    from tmp_tb
    """
        val table = tEnv.sqlQuery(sql)
        val rowDataDataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
        val rowDataDs = table.toDataStream[RowData](rowDataDataType)
        rowDataDs.addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var start = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                if(i % 1000000 == 0){
                    val s = (System.currentTimeMillis() - start).toDouble / 1000
                    println(i, s, i / s, TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                }
            }
        })
    }

    test("myjson3") {
        var sql = s"""
    CREATE TABLE tmp_tb(
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'app_pv',
        'properties.bootstrap.servers' = 'hadoop201:9092,hadoop202:9092,hadoop203:9092',
        'properties.auto.offset.reset' = 'latest',
        'properties.group.id' = 'test-group',
        'properties.enable.auto.commit' = 'true',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'myjson'
    )
    """
        tEnv.executeSql(sql)

        sql = """
    select
        *
    from tmp_tb
    """
        val table = tEnv.sqlQuery(sql)
        val rowDataDataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
        val rowDataDs = table.toDataStream[RowData](rowDataDataType)
        rowDataDs.addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var start = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                if(i % 1000000 == 0){
                    val s = (System.currentTimeMillis() - start).toDouble / 1000
                    println(i, s, i / s, TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                }
            }
        })
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
                val json = s"""{"dt": "$dt", "bs": "$bs", "data": "${str}.${i}", "report_time": "${time}.${i}", "browser_uniq_id": "$browser_uniq_id", "os_plant": "$os_plant", "page_id": "$page_id", "page_name": "HotelRN_Detail_Activity", "page_param": "$page_param", "item_id": $i, "item_type": 0, "visit_time": "$time"}\n"""
                val bytes = json.getBytes("utf-8")
                ctx.collect(bytes)
                ctx.collect(bytes)
                i += 1
            }
        }

        override def cancel(): Unit = this.stop = true
    }

    test("sourcecout") {
        val ds = env.addSource(getSource())
        ds.addSink(new RichSinkFunction[Array[Byte]] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: Array[Byte], context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, i / s, s2, i2 / s2,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("myjson4") {
        val ds = env.addSource(getSource()).disableChaining()
        val dataType = tEnv.createDataType("""row<dt string,
                                             |        bs string,
                                             |        report_time string,
                                             |        browser_uniq_id string,
                                             |        os_plant string,
                                             |        page_id string,
                                             |        page_name string,
                                             |        page_param string,
                                             |        item_id bigint,
                                             |        item_type int,
                                             |        visit_time string>""".stripMargin, true)
        assert(dataType.getLogicalType.isInstanceOf[RowType], "schema必须是row类型")
        val rowType = dataType.getLogicalType.asInstanceOf[RowType]
        val rowDataTypeInfo = InternalTypeInfo.of[RowData](dataType.getLogicalType) // ExternalTypeInfo.of(dataType)

        val deserializer = new scala.connector.format.JsonRowDataDeserializationSchema(rowType, rowDataTypeInfo)
        ds.map(new DeserializeFunc(deserializer))(rowDataTypeInfo).addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("json4") {
        val ds = env.addSource(getSource()).disableChaining()
        val dataType = tEnv.createDataType("""row<dt string,
                                             |        bs string,
                                             |        report_time string,
                                             |        browser_uniq_id string,
                                             |        os_plant string,
                                             |        page_id string,
                                             |        page_name string,
                                             |        page_param string,
                                             |        item_id bigint,
                                             |        item_type int,
                                             |        visit_time string>""".stripMargin, true)
        assert(dataType.getLogicalType.isInstanceOf[RowType], "schema必须是row类型")
        val rowType = dataType.getLogicalType.asInstanceOf[RowType]
        val rowDataTypeInfo = InternalTypeInfo.of[RowData](dataType.getLogicalType) // ExternalTypeInfo.of(dataType)

        val deserializer = new org.apache.flink.formats.json.JsonRowDataDeserializationSchema(rowType, rowDataTypeInfo, false, true, TimestampFormat.SQL)
        ds.map(new DeserializeFunc(deserializer))(rowDataTypeInfo).addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("myjson4simple") {
        val ds = env.addSource(getSource()).disableChaining()
        val dataType = tEnv.createDataType("""row<dt string,
                                             |        bs string,
                                             |        --report_time string,
                                             |        --browser_uniq_id string,
                                             |        os_plant string,
                                             |        page_id string,
                                             |        --page_name string,
                                             |        --page_param string,
                                             |        item_id bigint,
                                             |        item_type int,
                                             |        visit_time string>""".stripMargin, true)
        assert(dataType.getLogicalType.isInstanceOf[RowType], "schema必须是row类型")
        val rowType = dataType.getLogicalType.asInstanceOf[RowType]
        val rowDataTypeInfo = InternalTypeInfo.of[RowData](dataType.getLogicalType) // ExternalTypeInfo.of(dataType)

        val deserializer = new JsonRowDataSimpleDeserializationSchema(rowType, rowDataTypeInfo)
        ds.map(new DeserializeFunc(deserializer))(rowDataTypeInfo).addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("myjson4simplejava") {
        val ds = env.addSource(getSource()).disableChaining()
        val dataType = tEnv.createDataType("""row<dt string,
                                             |        bs string,
                                             |        report_time string,
                                             |        browser_uniq_id string,
                                             |        os_plant string,
                                             |        page_id string,
                                             |        page_name string,
                                             |        page_param string,
                                             |        item_id bigint,
                                             |        item_type int,
                                             |        visit_time string>""".stripMargin, true)
        assert(dataType.getLogicalType.isInstanceOf[RowType], "schema必须是row类型")
        val rowType = dataType.getLogicalType.asInstanceOf[RowType]
        val rowDataTypeInfo = InternalTypeInfo.of[RowData](dataType.getLogicalType) // ExternalTypeInfo.of(dataType)

        val deserializer = new JsonRowDataSimpleDeserializationSchemaJava(rowType, rowDataTypeInfo)
        ds.map(new DeserializeFunc(deserializer))(rowDataTypeInfo).addSink(new RichSinkFunction[RowData] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("myjson4javaBean") {
        val ds = env.addSource(getSource()).disableChaining()

        ds.map(new RichMapFunction[Array[Byte], JavaBean] {
            var jsonDeserialization: JsonJavaBeanDeserialization[JavaBean] = _

            override def open(parameters: Configuration): Unit = jsonDeserialization = new JsonJavaBeanDeserialization[JavaBean](classOf[JavaBean], true)

            override def map(value: Array[Byte]): JavaBean = {
                jsonDeserialization.deserialize(value)
            }
        }).addSink(new RichSinkFunction[JavaBean] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: JavaBean, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("myjson4FastJson2javaBean") {
        val ds = env.addSource(getSource()).disableChaining()

        ds.map(new RichMapFunction[Array[Byte], JavaBean] {
            var jsonDeserialization: FastJson2JavaBeanDeserialization[JavaBean] = _

            override def open(parameters: Configuration): Unit = jsonDeserialization = new FastJson2JavaBeanDeserialization[JavaBean](classOf[JavaBean], true)

            override def map(value: Array[Byte]): JavaBean = {
                jsonDeserialization.deserialize(value)
            }
        }).addSink(new RichSinkFunction[JavaBean] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: JavaBean, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("myjson4javaBean_fastjson") {
        val ds = env.addSource(getSource()).disableChaining()

        ds.map(new RichMapFunction[Array[Byte], JavaBean] {
            override def map(value: Array[Byte]): JavaBean = {
               JSON.parseObject(value, classOf[JavaBean])
            }
        }).addSink(new RichSinkFunction[JavaBean] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: JavaBean, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("myjson4javaBean_fastjson2") {
        val ds = env.addSource(getSource()).disableChaining()

        ds.map(new RichMapFunction[Array[Byte], JavaBean] {
            override def map(value: Array[Byte]): JavaBean = {
                com.alibaba.fastjson2.JSON.parseObject(value, classOf[JavaBean])
            }
        }).addSink(new RichSinkFunction[JavaBean] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: JavaBean, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("myjson4javaBean_fastjson22") {
        val ds = env.addSource(getSource()).disableChaining()

        ds.map(new RichMapFunction[Array[Byte], com.alibaba.fastjson2.JSONObject] {
            override def map(value: Array[Byte]): com.alibaba.fastjson2.JSONObject = {
                com.alibaba.fastjson2.JSON.parseObject(value)
            }
        }).addSink(new RichSinkFunction[com.alibaba.fastjson2.JSONObject] {
            var i = 0L
            var i2 = 0L
            var start = 0L
            var start2 = 0L
            override def open(parameters: Configuration): Unit = {
                start = System.currentTimeMillis()
                start2 = System.currentTimeMillis()
            }
            override def invoke(value: com.alibaba.fastjson2.JSONObject, context: SinkFunction.Context): Unit = {
                i += 1
                i2 += 1
                if(i % 1000000 == 0){
                    val end = System.currentTimeMillis()
                    val s = (end - start).toDouble / 1000
                    val s2 = (end - start2).toDouble / 1000
                    println(i, s, (i / s).toInt, s2, (i2 / s2).toInt,TsUtils.timestamp(System.currentTimeMillis()))
                    println(value)
                    start2 = end
                    i2 = 0
                }
            }
        })
    }

    test("fastjson22") {

    }
}

object JsonFormatSuite{
    class JavaBean extends Serializable {
        @BeanProperty
        var dt: String = _
        @BeanProperty
        var bs: String = _
        @BeanProperty
        var report_time: String = _
        @BeanProperty
        var browser_uniq_id: String = _
        @BeanProperty
        var os_plant: String = _
        @BeanProperty
        var page_id: String = _
        @BeanProperty
        var page_name: String = _
        @BeanProperty
        var page_param: String = _
        @BeanProperty
        var item_id: java.lang.Long = _
        @BeanProperty
        var item_type: java.lang.Integer = _
        @BeanProperty
        var visit_time: String = _

        override def toString = s"JavaBean($dt, $bs, $report_time, $browser_uniq_id, $os_plant, $page_id, $page_name, $page_param, $item_id, $item_type, $visit_time)"
    }

}