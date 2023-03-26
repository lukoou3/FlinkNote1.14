package scala.connector.format2;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;

public class JsonRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private RowType rowType;
    private TypeInformation<RowData> resultTypeInfo;
    private ObjectMapper objectMapper = new ObjectMapper();
    private GenericRowData row;

    public JsonRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo) {
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
        row = new GenericRowData(rowType.getFields().size());
        rowType.getFields().stream().map(field -> createConverter(field.getType().getTypeRoot()));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        return null;
    }

    private ValueConverter createConverter(LogicalTypeRoot tpe) {
        switch (tpe){
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + tpe);
        }
    }

    private StringData convertToString(JsonParser jsonParser) {
        return null;
    }

    private StringData convertToString(JsonNode jsonNode) {
        if (jsonNode.isContainerNode()) {
            return StringData.fromString(jsonNode.toString());
        } else {
            return StringData.fromString(jsonNode.asText());
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(JsonParser jsonParser);
    }
}
