package scala.sql.json;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import org.apache.commons.io.FileUtils;
import org.apache.flink.table.data.GenericRowData;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class JsonRateTest {

    public List<byte[]> getLines()throws Exception {
        String path = "D:\\IdeaWorkspace\\java-utils\\spark-utils\\datas\\mo_j2011_1_pv.txt";
        List<byte[]> lines = FileUtils.readLines(new File(path), StandardCharsets.UTF_8).stream()
                .filter(x -> !x.trim().isEmpty()).map(str -> str.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        return lines;
    }

    public List<String> getLines2()throws Exception {
        String path = "D:\\IdeaWorkspace\\java-utils\\spark-utils\\datas\\mo_j2011_1_pv.txt";
        List<String> lines = FileUtils.readLines(new File(path), StandardCharsets.UTF_8).stream()
                .filter(x -> !x.trim().isEmpty()).collect(Collectors.toList());
        return lines;
    }

    @Test
    public void toNode() throws Exception {
        List<byte[]> lines = getLines();
        GenericRowData row = new GenericRowData(2);
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(lines.size());
        long start = System.currentTimeMillis();
        for (int i = 0; i <= 1000; i++) {
            for (byte[] line : lines) {

                JsonNode jsonNode = objectMapper.readTree(line);
                JsonNode node = jsonNode.get("page_id");
                String pageId = null;
                String ctp = null;
                if(node != null){
                    pageId = node.asText();
                }
                node = jsonNode.get("ctp");
                if(node != null){
                    ctp = node.asText();
                }
                /*if(i == 100 || i == 500){
                    System.out.println(ctp);
                }*/
                row = new GenericRowData(2);
                row.setField(0, pageId);
                row.setField(1, ctp);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    public void jsonParser() throws Exception {
        List<byte[]> lines = getLines();
        GenericRowData row = new GenericRowData(2);
        JsonFactory jsonFactory = new JsonFactoryBuilder()
                // The two options below enabled for Hive compatibility
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
                .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
                .build();
        System.out.println(lines.size());
        long start = System.currentTimeMillis();
        for (int i = 0; i <= 1000; i++) {
            for (byte[] line : lines) {
                try(JsonParser jsonParser = jsonFactory.createParser(line)){
                    String pageId = null;
                    String ctp = null;

                    JsonToken nextToken = jsonParser.nextToken();
                    while(nextToken != JsonToken.END_OBJECT){
                        if(nextToken != JsonToken.FIELD_NAME){
                            nextToken = jsonParser.nextToken();
                            continue;
                        }
                        nextToken = jsonParser.nextToken();
                        String filedName = jsonParser.getCurrentName();
                        if (filedName.equals("page_id")) {
                            pageId = jsonParser.getText();
                        }
                        if (filedName.equals("ctp")) {
                            ctp = jsonParser.getText();
                        }
                        row.setField(0, pageId);
                        row.setField(1, ctp);
                    }
                    /*if(i == 1){
                        System.out.println(pageId);
                        System.out.println(ctp);
                    }*/
                }
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    public void jsonObject() throws Exception {
        List<byte[]> lines = getLines();
        GenericRowData row = new GenericRowData(2);
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(lines.size());
        long start = System.currentTimeMillis();
        for (int i = 0; i <= 1000; i++) {
            for (byte[] line : lines) {
                JSONObject jsonObject = JSON.parseObject(new String(line, StandardCharsets.UTF_8));
                String pageId = jsonObject.getString("page_id");
                String ctp = jsonObject.getString("ctp");
                /*if(i == 100 || i == 500){
                    System.out.println(ctp);
                }*/
                //row = new GenericRowData(2);
                row.setField(0, pageId);
                row.setField(1, ctp);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    public void jsonObject2() throws Exception {
        List<String> lines = getLines2();
        GenericRowData row = new GenericRowData(2);
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(lines.size());
        long start = System.currentTimeMillis();
        for (int i = 0; i <= 1000; i++) {
            for (String line : lines) {
                JSONObject jsonObject = JSON.parseObject(line);
                String pageId = jsonObject.getString("page_id");
                String ctp = jsonObject.getString("ctp");
                /*if(i == 100 || i == 500){
                    System.out.println(ctp);
                }*/
                //row = new GenericRowData(2);
                row.setField(0, pageId);
                row.setField(1, ctp);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    public void jsonElement() throws Exception {
        List<String> lines = getLines2();
        GenericRowData row = new GenericRowData(2);
        com.google.gson.JsonParser  jsonParser = new com.google.gson.JsonParser();
        System.out.println(lines.size());
        long start = System.currentTimeMillis();
        for (int i = 0; i <= 1000; i++) {
            for (String line : lines) {
                JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();
                JsonElement node = jsonObject.get("page_id");
                String pageId = null;
                String ctp = null;
                if(node != null){
                    pageId = node.getAsString();
                }
                node = jsonObject.get("ctp");
                if(node != null){
                    ctp = node.getAsString();
                }
                /*if(i == 1){
                    System.out.println(ctp);
                }*/
                //row = new GenericRowData(2);
                row.setField(0, pageId);
                row.setField(1, ctp);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
