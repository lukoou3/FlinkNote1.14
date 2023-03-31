package scala.sql.json;

//import com.dslplatform.json.DslJson;
//import com.dslplatform.json.runtime.Settings;
import com.google.gson.Gson;
//import com.jsoniter.JsonIterator;
//import com.jsoniter.spi.DecodingMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import scala.serialization.BinarySchema;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class JsonLogParseDataStream {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = null;
        int parallelism = parameter.getInt("parallelism", 0);
        String jsonFormat = parameter.get("json_format", "jackson");
        String startupMode = parameter.get("startup_mode", "earliest-offset");
        String groupId = "test";
        //groupId = "test-myjson";

        if(parameter.getInt("local", 1) == 1){
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }else{
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.getConfig().enableObjectReuse();
        if(parallelism > 0){
            env.setParallelism(parallelism);
        }

        OffsetsInitializer startingOffsetsInitializer = OffsetsInitializer.earliest();
        if(startupMode.equals("latest-offset")){
            startingOffsetsInitializer = OffsetsInitializer.latest();
        }else if(startupMode.equals("group-offsets")){
            startingOffsetsInitializer = OffsetsInitializer.committedOffsets();
        }

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.41.31:9092");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "2000");
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setProperties(properties)
                .setTopics("SESSION-RECORD")
                .setGroupId(groupId)
                .setStartingOffsets(startingOffsetsInitializer)
                .setValueOnlyDeserializer(new BinarySchema())
                .build();

        DataStreamSource<byte[]> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        RichMapFunction<byte[], LogData> jsonMapFunc = null;

        if("jackson".equals(jsonFormat)){
            jsonMapFunc = jacksonMapFunc(env);
        }else if("fastJson".equals(jsonFormat)){
            jsonMapFunc = fastJsonMapFunc(env);
        }else if("fastJson2".equals(jsonFormat)){
            jsonMapFunc = fastJson2MapFunc(env);
        }/*else if("dslJson".equals(jsonFormat)){
            jsonMapFunc = dslJsonMapFunc(env);
        }else if("gson".equals(jsonFormat)){
            jsonMapFunc = gsonMapFunc(env);
        }else if("jsoniter".equals(jsonFormat)){
            jsonMapFunc = jsoniterMapFunc(env);
        }else if("jsoniterCodegen".equals(jsonFormat)){
            jsonMapFunc = jsoniterCodegenMapFunc(env);
        }*/

        ds.map(jsonMapFunc).addSink(new RichSinkFunction<LogData>() {
            private int cnt = 0;
            private long start = 0L;
            private int tcp4Cnt = 0;
            private int tcp6Cnt = 0;
            private int udp4Cnt = 0;
            private int udp6Cnt = 0;
            private String tcp4 = "IPv4_TCP";
            private String tcp6 = "IPv6_TCP";
            private String udp4 = "IPv4_UDP";
            private String udp6 = "IPv6_UDP";

            @Override
            public void invoke(LogData data, Context context) throws Exception {
                long ts = System.currentTimeMillis();
                if(start == 0L){
                    start = ts;
                }
                cnt += 1;
                String protocol = data.getCommon_l4_protocol();
                if(tcp4.equals(protocol)){
                    tcp4Cnt += 1;
                }else if(tcp6.equals(protocol)){
                    tcp6Cnt += 1;
                }else if(udp4.equals(protocol)){
                    udp4Cnt += 1;
                }else if(udp6.equals(protocol)){
                    udp6Cnt += 1;
                }

                if(ts - start >= 10000){
                    String info = jsonFormat + ", " +new java.sql.Timestamp(ts).toString() + ", cnt:"+ cnt + ", tcp4Cnt:" + tcp4Cnt+ ", tcp6Cnt:"+ tcp6Cnt + ", udp4Cnt:" + udp4Cnt+ ", udp6Cnt:" + udp6Cnt;
                    System.out.println(info);
                    start = ts;
                    cnt = 0;
                    tcp4Cnt = 0;
                    tcp6Cnt = 0;
                    tcp6Cnt = 0;
                    udp6Cnt = 0;
                }
            }
        });
        //ds.map(jsonMapFunc).print();

        env.execute("JsonLogParseStream_" + jsonFormat);
    }

    public static RichMapFunction<byte[], LogData> fastJsonMapFunc(StreamExecutionEnvironment env) throws Exception {
        return new RichMapFunction<byte[], LogData>() {
            @Override
            public LogData map(byte[] bytes) throws Exception {
                return com.alibaba.fastjson.JSON.parseObject(bytes, LogData.class);
            }
        };
    }

    public static RichMapFunction<byte[], LogData> fastJson2MapFunc(StreamExecutionEnvironment env) throws Exception {
        return new RichMapFunction<byte[], LogData>() {
            @Override
            public LogData map(byte[] bytes) throws Exception {
                return com.alibaba.fastjson2.JSON.parseObject(bytes, LogData.class);
            }
        };
    }

    public static RichMapFunction<byte[], LogData> jacksonMapFunc(StreamExecutionEnvironment env) throws Exception {
        return new RichMapFunction<byte[], LogData>() {
            private ObjectMapper objectMapper;

            @Override
            public void open(Configuration parameters) throws Exception {
                objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }

            @Override
            public LogData map(byte[] bytes) throws Exception {
                return objectMapper.readValue(bytes, LogData.class);
            }
        };
    }

    /*public static RichMapFunction<byte[], LogData> dslJsonMapFunc(StreamExecutionEnvironment env) throws Exception {
        return new RichMapFunction<byte[], LogData>() {
            private DslJson<Object> dj;

            @Override
            public void open(Configuration parameters) throws Exception {
                dj = new DslJson<Object>(Settings.withRuntime());
            }

            @Override
            public LogData map(byte[] bytes) throws Exception {
                try {
                    return dj.deserialize(LogData.class, bytes, bytes.length);
                } catch (Exception e) {
                    System.out.println(new String(bytes, StandardCharsets.UTF_8));
                    throw e;
                }
            }
        };
    }*/

    public static RichMapFunction<byte[], LogData> gsonMapFunc(StreamExecutionEnvironment env) throws Exception {
        return new RichMapFunction<byte[], LogData>() {
            private Gson gson;

            @Override
            public void open(Configuration parameters) throws Exception {
                gson = new Gson();
            }

            @Override
            public LogData map(byte[] bytes) throws Exception {
                return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), LogData.class);
            }
        };
    }

    /*public static RichMapFunction<byte[], LogData> jsoniterMapFunc(StreamExecutionEnvironment env) throws Exception {
        return new RichMapFunction<byte[], LogData>() {
            @Override
            public LogData map(byte[] bytes) throws Exception {
                return JsonIterator.deserialize(bytes, LogData.class);
            }
        };
    }

    public static RichMapFunction<byte[], LogData> jsoniterCodegenMapFunc(StreamExecutionEnvironment env) throws Exception {
        return new RichMapFunction<byte[], LogData>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                JsonIterator.setMode(DecodingMode.DYNAMIC_MODE_AND_MATCH_FIELD_WITH_HASH);
            }

            @Override
            public LogData map(byte[] bytes) throws Exception {
                return JsonIterator.deserialize(bytes, LogData.class);
            }
        };
    }*/

}
