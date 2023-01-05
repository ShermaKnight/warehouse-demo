package org.example.event;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Date;
import java.util.Properties;

@SuppressWarnings("all")
public class EventJob {

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStateBackend(new FsStateBackend("hdfs://192.168.71.128:8020/flink/checkpoints/event"));
        CheckpointConfig config = environment.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(60000);

        DataStreamSource<String> textSource = environment.addSource(kafkaSource(false));
        KeyedStream<JSONObject, String> keyedStream = textSource.flatMap(new EventTextMap()).keyBy(new EventTextSelector());
        keyedStream.addSink(new EventRdsSink()).name("rds-sink");
        SingleOutputStreamOperator<String> streamOperator = keyedStream.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return value.toJSONString();
            }
        });
        streamOperator.addSink(dfsSink()).name("dfs-sink");
        streamOperator.addSink(kafkaSink()).name("kafka-sink");
        environment.execute();
    }

    private static FlinkKafkaConsumer<String> kafkaSource(boolean label) {
        Properties properties = new Properties();
        properties.setProperty("group.id", "flink-client");
        properties.setProperty("bootstrap.servers", "192.168.71.128:9092,192.168.71.129:9092,192.168.71.130:9092");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink", new SimpleStringSchema(), properties);
        if (label) {
            consumer.setStartFromEarliest();
        } else {
            consumer.setStartFromGroupOffsets();
        }
        return consumer;
    }

    private static FlinkKafkaProducer<String> kafkaSink() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.71.128:9092,192.168.71.129:9092,192.168.71.130:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("flink-downstream", new SimpleStringSchema(), properties);
        return producer;
    }

    private static StreamingFileSink<String> dfsSink() {
        DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy
                .create()
                .withMaxPartSize(1024 * 1024 * 120)
                .withRolloverInterval(Long.MAX_VALUE)
                .withInactivityInterval(60 * 1000)
                .build();

        return StreamingFileSink
                .forRowFormat(new Path("hdfs://192.168.71.128:8020/flink/event"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new BucketAssigner<String, String>() {
                    @Override
                    public String getBucketId(String s, Context context) {
                        return DateUtil.format(new Date(), "yyyy-MM-dd");
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(1000L)
                .build();
    }
}
