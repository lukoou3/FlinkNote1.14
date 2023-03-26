package scala.connector.format;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonRowDataSimpleDeserializationSchemaJava implements DeserializationSchema<RowData> {
    private RowType rowType;
    private TypeInformation<RowData> resultTypeInfo;
    transient private JsonFactory jsonFactory;
    transient private Map<String, Integer> names;
    transient private ValueConverter[] fieldConverters;
    transient private GenericRowData nullRow;
    transient private GenericRowData row;


    public JsonRowDataSimpleDeserializationSchemaJava(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo) {
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonFactory = new JsonFactoryBuilder()
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
                .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
                .build();
        List<RowType.RowField> fields = rowType.getFields();
        nullRow = new GenericRowData(fields.size());
        row = new GenericRowData(fields.size());
        names = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            names.put(fields.get(i).getName(), i);
        }
        fieldConverters = getFieldConverters(fields);
    }

    private ValueConverter[] getFieldConverters(List<RowType.RowField> fields) {
        return fields.stream().map(x -> x.getType()).map(this::makeConverter).toArray(ValueConverter[]::new);
    }

    private ValueConverter makeConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case INTEGER:
                return this::convertToInteger;
            case BIGINT:
                return this::convertToLong;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }

    private Object convertToString(JsonParser parser) throws Exception {
        JsonToken token = parser.getCurrentToken();
        switch (token) {
            case VALUE_STRING:
                return StringData.fromString(parser.getText());
            case VALUE_NUMBER_INT:
                return StringData.fromString(Long.toString(parser.getLongValue()));
            default:
                ByteArrayOutputStream writer = new ByteArrayOutputStream();
                try (JsonGenerator generator = jsonFactory.createGenerator(writer, JsonEncoding.UTF8)) {
                    generator.copyCurrentStructure(parser);
                }
                return StringData.fromBytes(writer.toByteArray());
        }
    }

    private Object convertToInteger(JsonParser parser) throws Exception {
        JsonToken token = parser.getCurrentToken();
        switch (token) {
            case VALUE_NUMBER_INT:
                return parser.getIntValue();
            case VALUE_STRING:
                return Integer.parseInt(parser.getText());
            default:
                throw new UnsupportedOperationException("Unsupported token: " + token);
        }
    }

    private Object convertToLong(JsonParser parser) throws Exception {
        JsonToken token = parser.getCurrentToken();
        switch (token) {
            case VALUE_NUMBER_INT:
                return parser.getLongValue();
            case VALUE_STRING:
                return Long.parseLong(parser.getText());
            default:
                throw new UnsupportedOperationException("Unsupported token: " + token);
        }
    }

    private Object convertToFloat(JsonParser parser) throws Exception {
        JsonToken token = parser.getCurrentToken();
        switch (token) {
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return parser.getFloatValue();
            case VALUE_STRING:
                return Float.parseFloat(parser.getText());
            default:
                throw new UnsupportedOperationException("Unsupported token: " + token);
        }
    }

    private Object convertToDouble(JsonParser parser) throws Exception {
        JsonToken token = parser.getCurrentToken();
        switch (token) {
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return parser.getDoubleValue();
            case VALUE_STRING:
                return Double.parseDouble(parser.getText());
            default:
                throw new UnsupportedOperationException("Unsupported token: " + token);
        }
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try (JsonParser parser = jsonFactory.createParser(message)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return nullRow;
            }

            int i = 0;
            while (i < names.size()) {
                row.setField(i, null);
                i += 1;
            }

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                    String jsonField = parser.getCurrentName();
                    Integer index = names.get(jsonField);
                    if (index != null) {
                        if (parser.nextToken() != JsonToken.VALUE_NULL) {
                            row.setField(index, fieldConverters[index].convert(parser));
                        }
                    }
                }
                parser.skipChildren();
            }

        } catch (Exception e) {
            e.printStackTrace();
            return nullRow;
        }

        return row;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(JsonParser parser) throws Exception;
    }
}
