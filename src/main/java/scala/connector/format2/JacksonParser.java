package scala.connector.format2;

import com.fasterxml.jackson.core.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class JacksonParser {
    private JsonFactory factory = new JsonFactory();
    private Function<JsonParser, RowData> rootConverter;

    public JacksonParser(LogicalType logicalType){

    }

    private Function<JsonParser, RowData> makeRootConverter(LogicalType logicalType){
        if(logicalType instanceof RowType){
            return makeStructRootConverter((RowType) logicalType);
        }else if (logicalType instanceof ArrayType){
            throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }else {
            throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }

    private Function<JsonParser, RowData> makeStructRootConverter(RowType rowType){
        ValueConverter[] fieldConverters = rowType.getFields().stream().map(x -> x.getType()).map(this::makeConverter).toArray(ValueConverter[]::new);
        Map<String, Integer> names = new HashMap<>();
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            names.put(fields.get(i).getName(), i);
        }
        Function<JsonToken, RowData> f = token -> {
            if(token == JsonToken.START_OBJECT){
                return null;
            }else{
                throw new RuntimeException("Parsing JSON arrays as structs is forbidden.");
            }
        };
        return parser ->{
            try {
                return parseJsonToken(parser, rowType, f);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private ValueConverter makeConverter(LogicalType logicalType){
        switch (logicalType.getTypeRoot()){
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }

    private StringData convertToString(JsonParser parser, LogicalType logicalType) throws IOException {
        Function<JsonToken, StringData> f = token -> {
            if(token == JsonToken.VALUE_STRING){
                try {
                    return StringData.fromString(parser.getText());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }else{
                // Note that it always tries to convert the data as string without the case of failure.
                ByteArrayOutputStream writer = new ByteArrayOutputStream();
                try(JsonGenerator generator = factory.createGenerator(writer, JsonEncoding.UTF8)) {
                    generator.copyCurrentStructure(parser);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
                return StringData.fromBytes(writer.toByteArray());
            }
        };
        return parseJsonToken(parser, logicalType, f);
    }

    private <T> T parseJsonToken(JsonParser parser, LogicalType logicalType, Function<JsonToken, T> f) throws IOException {
        T data = null;
        JsonToken token = parser.getCurrentToken();
        if(token == JsonToken.FIELD_NAME){
            parser.nextToken();
            data = parseJsonToken(parser, logicalType, f);
        } else if(token == null || token == JsonToken.VALUE_NULL) {
            data = null;
        }else{
            f.apply(token);
        }
        return data;
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(JsonParser parser, LogicalType logicalType) throws IOException;
    }


}
