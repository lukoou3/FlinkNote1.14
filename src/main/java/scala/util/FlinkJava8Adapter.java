package scala.util;

import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;

public class FlinkJava8Adapter {

    public static <T> InternalTypeInfo<T> logicalType2InternalTypeInfo(LogicalType type) {
        return InternalTypeInfo.of(type);
    }


}
