package com.java.connector.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SocketDynamicTableTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        /**
         * {"id":"1","name":"罗隐32","age":1300}
         * {"id":"1", "name":"罗隐", "age":30}
         * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
         */
        String sql = "CREATE TABLE tmp_tb1 (\n" +
                "      id int,\n" +
                "      name string,\n" +
                "      age int\n" +
                "      -- PRIMARY KEY (id) NOT ENFORCED\n" +
                "    ) WITH (\n" +
                "      'connector' = 'java-mysocket',\n" +
                "      -- 'hostname' = 'localhost',\n" +
                "      'hostname' = '192.168.41.31',\n" +
                "      'port' = '9999',\n" +
                "      'format' = 'json',\n" +
                "      -- format的参数配置，前面需要加format的名称\n" +
                "      'json.fail-on-missing-field' = 'false',\n" +
                "      -- json解析报错会直接返回null(row是null), 没法跳过忽略, {}不会报错, 属性都是null\n" +
                "      'json.ignore-parse-errors' = 'true'\n" +
                "    )";
        tEnv.executeSql(sql);

        sql = "select * from tmp_tb1";
        Table rstTable = tEnv.sqlQuery(sql);

        // 阻塞
        rstTable.execute().print();

        env.execute("SocketDynamicTableTest");
    }

}
