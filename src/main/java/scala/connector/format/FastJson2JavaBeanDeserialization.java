package scala.connector.format;

import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.util.Fnv;
import com.carrotsearch.hppc.LongIntHashMap;
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

public class FastJson2JavaBeanDeserialization<T>{
    private Class<T> clazz;
    private boolean objectReuse;
    private JsonFactory jsonFactory;
    private Map<Long, Integer> names;
    private long[] names2;
    private LongIntHashMap names3;
    private ValueConverter[] fieldConverters;
    private Field[] fields;
    private Class[] returnTypes;
    private ValueSetter[] fieldSetters;
    private ValueSetter[] fieldSetters2;
    private Object[] fieldDefaultValues;
    private boolean[] fieldHasValues;
    private T data;

    public FastJson2JavaBeanDeserialization(Class<T> clazz) throws Exception{
        this(clazz, true);
    }

    public FastJson2JavaBeanDeserialization(Class<T> clazz, boolean objectReuse) throws Exception{
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
        names3 = new LongIntHashMap(properties.length + properties.length);
        names2 = new long[properties.length];
        fieldConverters = new ValueConverter[properties.length];
        fieldDefaultValues = new Object[properties.length];
        fields = new Field[properties.length];
        returnTypes = new Class[properties.length];
        fieldSetters = new ValueSetter[properties.length];
        fieldSetters2 = new ValueSetter[properties.length];
        for (int i = 0; i < properties.length; i++) {
            //names.put(properties[i].getName(), i);
            names.put(Fnv.hashCode64(properties[i].getName()), i);
            names2[i] = Fnv.hashCode64(properties[i].getName());
            names3.put(Fnv.hashCode64(properties[i].getName()), i);
            fieldConverters[i] = makeConverter(properties[i]);
            fieldDefaultValues[i] = fieldDefaultValue(properties[i]);
            Field field = clazz.getDeclaredField(properties[i].getName());
            field.setAccessible(true);
            fields[i] = field;
            returnTypes[i] = properties[i].getReadMethod().getReturnType();
            fieldSetters[i] = field::set;
            fieldSetters2[i] = properties[i].getWriteMethod()::invoke;
        }
        fieldHasValues = new boolean[properties.length];
        if (objectReuse) {
            data = clazz.newInstance();
        }
    }

    public T deserialize(byte[] bytes) throws Exception {
        try (JSONReader reader = JSONReader.of(bytes)) {
            if (!reader.nextIfMatch('{')) {
                throw new Exception("json格式不正确");
            }

            if(!objectReuse){
                data = clazz.newInstance();
            }else{
                for (int i = 0; i < fieldHasValues.length; i++) {
                    fieldHasValues[i] = false;
                }
            }

            while (!reader.nextIfMatch('}')) {
                //String jsonField = reader.readFieldName();
                /*Long jsonField = reader.readFieldNameHashCode();
                Integer index = names.get(jsonField);
                if (index != null) {
                    fieldHasValues[index] = true;
                    fieldSetters[index].set(data, fieldConverters[index].convert(reader));
                }else{
                    reader.skipValue();
                }*/
                /*long jsonField = reader.readFieldNameHashCode();
                boolean flag = false;
                for (int index = 0; index < names2.length; index++) {
                    if(jsonField == names2[index]){
                        fieldHasValues[index] = true;
                        fieldSetters[index].set(data, fieldConverters[index].convert(reader));
                        flag = true;
                        break;
                    }
                }
                if(!flag){
                    reader.skipValue();
                }*/
                long jsonField = reader.readFieldNameHashCode();
                int index = names3.get(jsonField);
                if (index != 0) {
                    fieldHasValues[index] = true;
                    //fieldConverters[index].convert(reader);
                    //fieldSetters[index].set(data, fieldConverters[index].convert(reader));
                    //fields[index].set(data, fieldConverters[index].convert(reader));
                    //reader.readString();
                    //reader.skipValue();
                    Class returnType = returnTypes[index];
                    Object value;
                    if(returnType == String.class){
                        value = reader.readString();
                    } else if (returnType == Integer.TYPE || returnType == Integer.class){
                        value = reader.readInt32();
                    } else if (returnType == Long.TYPE || returnType == Long.class){
                        value = reader.readInt64();
                    } else if (returnType == Float.TYPE || returnType == Float.class){
                        value = reader.readFloat();
                    } else if (returnType == Double.TYPE || returnType == Double.class){
                        value = reader.readDouble();
                    }else{
                        throw new UnsupportedOperationException("Unsupported type: " + returnType);
                    }
                    fields[index].set(data, value);

                    //fieldSetters2[index].set(data, fieldConverters[index].convert(reader));
                }else{
                    reader.skipValue();
                }

            }

            /*if(objectReuse){
                for (int i = 0; i < fieldHasValues.length; i++) {
                    if(!fieldHasValues[i]){
                        //fieldSetters[i].set(data, fieldDefaultValues[i]);
                        fields[i].set(data, fieldDefaultValues[i]);
                        //fieldSetters2[i].set(data, fieldDefaultValues[i]);
                    }
                }
            }*/

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

    private Object convertToString(JSONReader reader) throws Exception {
        return reader.readString();
    }

    private Object convertToInteger(JSONReader reader) throws Exception {
        return reader.readInt32();
    }

    private Object convertToLong(JSONReader reader) throws Exception {
        return reader.readInt64();
    }

    private Object convertToFloat(JSONReader reader) throws Exception {
        return reader.readFloat();
    }

    private Object convertToDouble(JSONReader reader) throws Exception {
        return reader.readDouble();
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
        Object convert(JSONReader reader) throws Exception;
    }

    @FunctionalInterface
    public interface ValueSetter extends Serializable {
        void set(Object obj, Object value) throws Exception;
    }
}
