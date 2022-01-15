package org.example.buried;

import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

@SuppressWarnings("all")
public class BuriedJob {

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStateBackend(new FsStateBackend("hdfs://192.168.71.128:8020/flink/checkpoints/buried"));
        CheckpointConfig config = environment.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(60000);

        DataStreamSource<String> textSource = environment.addSource(kafkaSource(true));
        textSource.flatMap(new BuriedTextMap()).keyBy(new BuriedTextSelector()).addSink(new BuriedRdsSink());
        environment.execute();
    }

    private static FlinkKafkaConsumer<String> kafkaSource(boolean label) {
        Properties properties = new Properties();
        properties.setProperty("group.id", "flink-client");
        properties.setProperty("bootstrap.servers", "192.168.71.128:9092,192.168.71.129:9092,192.168.71.130:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink", new SimpleStringSchema(), properties);
        if (label) {
            consumer.setStartFromEarliest();
        } else {
            consumer.setStartFromGroupOffsets();
        }
        return consumer;
    }
}
