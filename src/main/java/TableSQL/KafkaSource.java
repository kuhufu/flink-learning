package TableSQL;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.*;

public class KafkaSource {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE person (" +
                "    name       VARCHAR," +
                "    amount     BIGINT" +
                ") WITH (" +
                "    'connector' = 'kafka'," +
                "    'topic'     = 'test_kafka'," +
                "    'properties.bootstrap.servers' = '192.168.200.202:9092'," +
                "    'properties.group.id' = 'test'," +
                "    'format'    = 'csv'" +
                ")");


        tEnv.executeSql("CREATE TABLE spend_report (" +
                "    name       VARCHAR," +
                "    total      BIGINT," +
                "    PRIMARY KEY (name) NOT ENFORCED" +
                ") WITH (" +
                "   'connector'  = 'jdbc'," +
                "   'url'        = 'jdbc:mysql://123.57.139.215:3316/flinkdb'," +
                "   'table-name' = 'spend_report'," +
                "   'driver'     = 'com.mysql.jdbc.Driver'," +
                "   'username'   = 'root'," +
                "   'password'   = 'onekick123456'" +
                ")");

        tEnv.from("person")
                .groupBy($("name"))
                .select($("name"), $("amount").sum().as("total"))
                .executeInsert("spend_report");
    }
}
