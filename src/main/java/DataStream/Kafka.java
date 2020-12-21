package DataStream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class Kafka {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var tEnv = StreamTableEnvironment.create(env);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.200.202:9092");
        properties.setProperty("group.id", "test");

        var consumer = new FlinkKafkaConsumer<>("test_kafka", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();

        var stream = env.addSource(consumer)
                .print();

        env.execute("kafka");
    }
}
