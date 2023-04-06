package scala.connector.format;

import com.jsoniter.JsonIterator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestJsoniter {

    public static void main(String[] args) throws IOException {
        byte[] bytes = "{\"id\":\"1\", \"name\":\"罗隐\",\"text\":{\"name\": 11  , \"age\":30}, \"age\":30}".getBytes(StandardCharsets.UTF_8);
        //bytes = "{\"id\":\"1\", \"name\":\"罗隐\",\"text\":{\"name\": 11  , \"age\":30}, \"age\":null}".getBytes(StandardCharsets.UTF_8);
        JsonIterator iter = JsonIterator.parse(bytes);
        for (String field = iter.readObject(); field != null; field = iter.readObject()) {
            switch (field) {
                case "id":
                    System.out.println(iter.readString());
                    continue;
                case "name":
                    System.out.println(iter.readString());
                    continue;
                case "age":
                    if(iter.readNull()){
                        System.out.println("null");
                    }else{
                        System.out.println(iter.readInt());
                    }
                    continue;
                case "text":
                    System.out.println(iter.readAny());
                    continue;
                default:
                    iter.skip();
            }
        }
    }


}
