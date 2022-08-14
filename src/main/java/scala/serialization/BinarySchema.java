package scala.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class BinarySchema implements DeserializationSchema<byte[]>, SerializationSchema<byte[]> {
    @Override
    public byte[] serialize(byte[] body) {
        return body;
    }

    @Override
    public byte[] deserialize(byte[] body) throws IOException {
        return body;
    }

    @Override
    public boolean isEndOfStream(byte[] bytes) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}
