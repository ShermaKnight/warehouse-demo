package org.example.stream;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class LogJob {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textSource = environment.addSource(kafkaSource(false));
        KeyedStream<JSONObject, String> keyedStream = textSource.flatMap(new LogTextMap()).keyBy(new LogTextSelector());

//        keyedStream.addSink(new LogRdsSink()).name("rds-sink");
        SingleOutputStreamOperator<String> streamOperator = keyedStream.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return value.toJSONString();
            }
        });
        streamOperator.addSink(kafkaSink()).name("kafka-sink");
        environment.execute();
    }

    private static FlinkKafkaConsumer<String> kafkaSource(boolean label) {
        Properties properties = new Properties();
        properties.setProperty("group.id", "flink-client");
        properties.setProperty("bootstrap.servers", "kafka-01:9092,kafka-02:9092,kafka-03:9092");
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
        properties.setProperty("bootstrap.servers", "kafka-01:9092,kafka-02:9092,kafka-03:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("flink-downstream", new SimpleStringSchema(), properties);
        return producer;
    }

}
