package TableSQL;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;

import static org.apache.flink.table.api.Expressions.*;

public class UDFSource {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);


        var person = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
                        DataTypes.FIELD("amount", DataTypes.BIGINT()),
                        DataTypes.FIELD("event_time", DataTypes.BIGINT())
                ),
                row("a", 1L, 1608707925000L),
                row("b", 1L, 1608707926000L),
                row("a", 1L, 1608707926000L)
        );

        person.addOrReplaceColumns($("proc_time").as("PROCTIME()"));
        

        tEnv.executeSql("CREATE TABLE spend_report (" +
                "   name       VARCHAR," +
                "   total      BIGINT," +
                "   ts         TIMESTAMP," +
                "   PRIMARY KEY (name) NOT ENFORCED" +
                ") WITH (" +
                "   'connector'  = 'print'" +
                ")");

        tEnv.from("person")
                .window(Tumble.over(lit(2).second()).on($("in_ts")).as("w"))
                .groupBy($("name"))
                .select(
                        $("name"),
                        $("amount").sum().as("total"),
                        $("w").start().as("ts")

                )
                .executeInsert("spend_report");
    }
}
