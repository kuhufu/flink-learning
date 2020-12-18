package TableSQL;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.*;

public class MySQLSource {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE person (\n" +
                "    name           VARCHAR,\n" +
                "    amount         BIGINT\n" +
                ") WITH (\n" +
                "   'connector'  = 'jdbc',\n" +
                "   'url'        = 'jdbc:mysql://123.57.139.215:3316/flinkdb',\n" +
                "   'table-name' = 'person',\n" +
                "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "   'username'   = 'root',\n" +
                "   'password'   = 'onekick123456'\n" +
                ")");

        //sink的表必须要有主键
        tEnv.executeSql("CREATE TABLE spend_report (\n" +
                "    name       VARCHAR,\n" +
                "    total      BIGINT,\n" +
                "    PRIMARY KEY (name) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector'  = 'jdbc',\n" +
                "   'url'        = 'jdbc:mysql://123.57.139.215:3316/flinkdb',\n" +
                "   'table-name' = 'spend_report',\n" +
                "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "   'username'   = 'root',\n" +
                "   'password'   = 'onekick123456'\n" +
                ")");

        tEnv.from("person")
                .groupBy($("name"))
                .select($("name"), $("amount").sum().as("total"))
                .executeInsert("spend_report");
    }
}
