package scala.stream.util;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 其实不用做这个转换了，scala编译时加上"-target:jvm-1.8"参数就行
 * 不过得使用net.alchim31.maven编译插件
 */
public class FlinkJava8Interface {
    public static OffsetsInitializer earliestOffsetsInitializer(){
        return OffsetsInitializer.earliest();
    }

    public static OffsetsInitializer committedOffsetsInitializer(OffsetResetStrategy offsetResetStrategy){
        return OffsetsInitializer.committedOffsets(offsetResetStrategy);
    }
}
