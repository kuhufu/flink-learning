package DataStream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Table {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var tEnv = StreamTableEnvironment.create(env);


        var dataStream = env
                .fromElements("a 1", "a 2", "a 3", "a 1", "a 5", "a 3", "a 1")
                .map(v -> v.split(" "))
                .filter(v -> v.length == 2)
                .map(v -> Tuple2.of(v[0], Integer.parseInt(v[1])))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        dataStream.print();
        tEnv.createTemporaryView("fruits", dataStream, $("name"), $("num"));

        var resultTable = tEnv.sqlQuery("SELECT name, SUM(num) FROM fruits GROUP BY name");

        tEnv.toRetractStream(resultTable, Row.class).print();

        env.execute("table");
    }
}
