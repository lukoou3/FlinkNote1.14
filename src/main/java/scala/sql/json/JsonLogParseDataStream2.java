package scala.sql.json;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.connector.localfile.LocalFileSinkFunction;
import scala.serialization.BinarySchema;

import java.util.Properties;

public class JsonLogParseDataStream2 {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = null;
        int parallelism = parameter.getInt("parallelism", 0);
        String startupMode = parameter.get("startup_mode", "earliest-offset");
        String groupId = "test";
        //groupId = "test-myjson";

        if(parameter.getInt("local", 1) == 1){
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }else{
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);
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

        ds.addSink(new LocalFileSinkFunction(
                "D:\\doc\\logs.json",
                new BinarySchema()
        ));

        env.execute("11");
    }

}
