package scala.connector.format;

import com.alibaba.fastjson2.JSONB;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.util.Fnv;

import java.nio.charset.StandardCharsets;

import static com.alibaba.fastjson2.JSONB.Constants.BC_OBJECT;
import static com.alibaba.fastjson2.JSONB.Constants.BC_OBJECT_END;

public class TestFastJson2 {

    public static void main(String[] args) {
        // {"id":"1", "name":"罗隐","text":{"name": 11  , "age":30}, "age":30}
        byte[] bytes = "{\"id\":\"1\", \"name\":\"罗隐\",\"text\":{\"name\": 11  , \"age\":30}, \"age\":30}".getBytes(StandardCharsets.UTF_8);
        try (JSONReader reader = JSONReader.of(bytes)) {
            if (reader.nextIfNull()) {
                return ;
            }

            if (!reader.nextIfMatch('{')) {
                throw new JSONException("expect object, but " + JSONB.typeName(reader.getType()));
            }

            for (; ; ) {
                if (reader.nextIfMatch('}')) {
                    break;
                }

                String fieldName = reader.readFieldName();
                System.out.println(fieldName);
                reader.skipValue();
            }
        }

        try (JSONReader reader = JSONReader.of(bytes)) {
            if (reader.nextIfNull()) {
                return ;
            }

            if (!reader.nextIfMatch('{')) {
                throw new JSONException("expect object, but " + JSONB.typeName(reader.getType()));
            }

            for (; ; ) {
                if (reader.nextIfMatch('}')) {
                    break;
                }

                long nameHashCode = reader.readFieldNameHashCode();
                System.out.println(nameHashCode);
                reader.skipValue();
            }
        }

        System.out.println("id:" + Fnv.hashCode64("id"));
        System.out.println("name:" + Fnv.hashCode64("name"));
        System.out.println("text:" + Fnv.hashCode64("text"));
        System.out.println("age:" + Fnv.hashCode64("age"));
    }

    public static void main2(String[] args) {
        // {"id":"1", "name":"罗隐","text":{"name": 11  , "age":30}, "age":30}
        byte[] bytes = "{\"id\":\"1\", \"name\":\"罗隐\",\"text\":{\"name\": 11  , \"age\":30}, \"age\":30}".getBytes(StandardCharsets.UTF_8);
        try (JSONReader reader = JSONReader.of(bytes)) {
            if (reader.nextIfNull()) {
                return ;
            }

            if (!reader.nextIfMatch('{')) {
                throw new JSONException("expect object, but " + JSONB.typeName(reader.getType()));
            }

            for (; ; ) {
                if (reader.nextIfMatch('}')) {
                    break;
                }

                String fieldName = "reader.readFieldName()";
                //String fieldName = reader.readFieldName();
                long nameHashCode = reader.readFieldNameHashCode();
                System.out.println(nameHashCode);
                System.out.println("age:" + Fnv.hashCode64("age"));
                System.out.println("id:" + Fnv.hashCode64("age"));
                System.out.println("text:" + Fnv.hashCode64("age"));
                if(fieldName.equals("id") || fieldName.equals("age") || fieldName.equals("text")){
                    String value = reader.readString();
                    //reader.readInt32();
                    System.out.println(fieldName + ":" + value);
                }else{
                    reader.skipValue();
                }


            }
        }
    }

}
