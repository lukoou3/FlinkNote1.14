package scala.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class BinarySerializationSchema implements SerializationSchema<byte[]>{
    @Override
    public byte[] serialize(byte[] body) {
        return body;
    }
}
