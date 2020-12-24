package TableSQL;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class DataGenSource {
    public static void main(String[] args) throws Exception {

        var settings = EnvironmentSettings.newInstance().build();
        var tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE person (" +
                "    name           VARCHAR," +
                "    amount         BIGINT," +
                "    event_time     BIGINT," +
                "    in_ts AS TO_TIMESTAMP(FROM_UNIXTIME(event_time/1000))," +
                "    WATERMARK FOR in_ts AS in_ts - INTERVAL '1' SECOND" +
                ") WITH (" +
                "    'connector' = 'datagen'," +
                "    'rows-per-second' = '3'," +
                "    'fields.name.start' = 'a'," +
                "    'fields.name.end' = 'z'," +
                "    'fields.name.length' = '1'," +

                "    'fields.amount.min' = '1'," +
                "    'fields.amount.max' = '10'," +

                "    'fields.event_time.kind' = 'random'," +
                "    'fields.event_time.start' = '1608719821693'," +
                "    'fields.event_time.end' = '1608729821693'," +
                "    'fields.event_time.min' = '1608719821693'," +
                "    'fields.event_time.max' = '1608729821693'" +
                ")");

        tEnv.executeSql("CREATE TABLE spend_report (" +
                "    name       VARCHAR," +
                "    total      BIGINT," +
                "    report_ts      TIMESTAMP," +
                "    PRIMARY KEY (name, report_ts) NOT ENFORCED" +
                ") WITH (" +
                "   'connector'  = 'print'" +
                ")");

        tEnv.from("person")
                .window(Tumble.over(lit(10).second()).on($("in_ts")).as("w"))
                .groupBy($("name"), $("w"))
                .select(
                        $("name"),
                        $("amount").sum().as("total"),
                        $("w").start().as("report_ts")
                )
                .executeInsert("spend_report");
    }
}
