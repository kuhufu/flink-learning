package DataStream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Jdbc {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval()

        var tEnv = StreamTableEnvironment.create(env);


        var dataStream = env.fromElements(
                Tuple2.of(1, System.currentTimeMillis() + 1000),
                Tuple2.of(1, System.currentTimeMillis() + 2000),
                Tuple2.of(1, System.currentTimeMillis() + 3000))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, ts) -> event.f1)
                )
                .addSink(JdbcSink.sink(
                        "INSERT INTO person(name, amount) VALUES(?, ?)",
                        (ps, v) -> {
                            ps.setString(1, String.valueOf(v.f0));
                            ps.setInt(2, Math.toIntExact(v.f1));
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://123.57.139.215:3316/flinkdb")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("onekick123456")
                                .build()
                ));

        env.execute("jdbc");
    }
}
