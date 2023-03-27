package scala.connector.format;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JsonJavaBeanDeserialization<T>{
    private Class<T> clazz;
    private boolean objectReuse;
    private JsonFactory jsonFactory;
    private Map<String, Integer> names;
    private ValueConverter[] fieldConverters;
    private ValueSetter[] fieldSetters;
    private Object[] fieldDefaultValues;
    private boolean[] fieldHasValues;
    private T data;

    public JsonJavaBeanDeserialization(Class<T> clazz) throws Exception{
        this(clazz, true);
    }

    public JsonJavaBeanDeserialization(Class<T> clazz, boolean objectReuse) throws Exception{
        this.clazz = clazz;
        this.objectReuse = objectReuse;
        init();
    }

    private void init() throws Exception{
        jsonFactory = new JsonFactoryBuilder()
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
                .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
                .build();
        PropertyDescriptor[] properties = getJavaBeanReadableAndWritableProperties(clazz);
        names = new HashMap<>();
        fieldConverters = new ValueConverter[properties.length];
        fieldDefaultValues = new Object[properties.length];
        fieldSetters = new ValueSetter[properties.length];
        for (int i = 0; i < properties.length; i++) {
            names.put(properties[i].getName(), i);
            fieldConverters[i] = makeConverter(properties[i]);
            fieldDefaultValues[i] = fieldDefaultValue(properties[i]);
            Field field = clazz.getDeclaredField(properties[i].getName());
            field.setAccessible(true);
            fieldSetters[i] = field::set;
        }
        fieldHasValues = new boolean[properties.length];
        if (objectReuse) {
            data = clazz.newInstance();
        }
    }

    public T deserialize(byte[] bytes) throws Exception {
        try (JsonParser parser = jsonFactory.createParser(bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new Exception("json格式不正确");
            }

            if(!objectReuse){
                data = clazz.newInstance();
            }else{
                for (int i = 0; i < fieldHasValues.length; i++) {
                    fieldHasValues[i] = false;
                }
            }

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                    String jsonField = parser.getCurrentName();
                    Integer index = names.get(jsonField);
                    if (index != null) {
                        if (parser.nextToken() != JsonToken.VALUE_NULL) {
                            fieldHasValues[index] = true;
                            fieldSetters[index].set(data, fieldConverters[index].convert(parser));
                        }
                    }
                }
                parser.skipChildren();
            }

            if(objectReuse){
                for (int i = 0; i < fieldHasValues.length; i++) {
                    if(!fieldHasValues[i]){
                        fieldSetters[i].set(data, fieldDefaultValues[i]);
                    }
                }
            }

        }

        return data;
    }

    private Object fieldDefaultValue(PropertyDescriptor property) {
        Class<?> returnType = property.getReadMethod().getReturnType();
        if(returnType == String.class || returnType == Integer.class || returnType == Long.class
                || returnType == Float.class || returnType == Double.class){
            return null;
        } else if (returnType == Integer.TYPE ){
            return 0;
        } else if (returnType == Long.TYPE){
            return 0L;
        } else if (returnType == Float.TYPE){
            return 0F;
        } else if (returnType == Double.TYPE){
            return 0D;
        }else{
            throw new UnsupportedOperationException("Unsupported type: " + returnType);
        }
    }

    private ValueConverter makeConverter(PropertyDescriptor property) {
        Class<?> returnType = property.getReadMethod().getReturnType();
        if(returnType == String.class){
            return this::convertToString;
        } else if (returnType == Integer.TYPE || returnType == Integer.class){
            return this::convertToInteger;
        } else if (returnType == Long.TYPE || returnType == Long.class){
            return this::convertToLong;
        } else if (returnType == Float.TYPE || returnType == Float.class){
            return this::convertToFloat;
        } else if (returnType == Double.TYPE || returnType == Double.class){
            return this::convertToDouble;
        }else{
            throw new UnsupportedOperationException("Unsupported type: " + returnType);
        }
    }

    private Object convertToString(JsonParser parser) throws Exception {
        JsonToken token = parser.getCurrentToken();
        switch (token) {
            case VALUE_STRING:
                return parser.getText();
            case VALUE_NUMBER_INT:
                return Long.toString(parser.getLongValue());
            default:
                ByteArrayOutputStream writer = new ByteArrayOutputStream();
                try (JsonGenerator generator = jsonFactory.createGenerator(writer, JsonEncoding.UTF8)) {
                    generator.copyCurrentStructure(parser);
                }
                return new String(writer.toByteArray(), StandardCharsets.UTF_8);
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

    private PropertyDescriptor[] getJavaBeanReadableAndWritableProperties(Class<T> clazz) throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
        return Arrays.stream(beanInfo.getPropertyDescriptors()).filter(x -> x.getName() != "class")
                .filter(x -> x.getName() != "declaringClass")
                .filter(x -> x.getReadMethod() != null)
                .filter(x -> x.getWriteMethod() != null)
                .toArray(PropertyDescriptor[]::new);
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(JsonParser parser) throws Exception;
    }

    @FunctionalInterface
    public interface ValueSetter extends Serializable {
        void set(Object obj, Object value) throws Exception;
    }
}
