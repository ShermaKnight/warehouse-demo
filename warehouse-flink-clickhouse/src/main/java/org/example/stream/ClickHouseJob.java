package org.example.stream;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Optional;

public class ClickHouseJob {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> kafkaSource = environment.fromSource(kafkaConsumer(), WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<String> streamOperator = kafkaSource.map((MapFunction<String, ClickHouseBean>) s -> {
            if (StringUtils.isNoneEmpty(s)) {
                return JSONObject.parseObject(s, ClickHouseBean.class);
            }
            return null;
        }).filter((FilterFunction<ClickHouseBean>) clickHouseBean -> Optional.ofNullable(clickHouseBean).isPresent()).map((MapFunction<ClickHouseBean, String>) clickHouseBean -> JSONObject.toJSONString(clickHouseBean));
        streamOperator.sinkTo(kafkaProducer());
        environment.execute();
    }

    private static KafkaSource<String> kafkaConsumer() {
        return KafkaSource.<String>builder()
                .setBootstrapServers("kafka-01:9092,kafka-02:9092,kafka-03:9092")
                .setTopics("flink")
                .setGroupId("flink-client")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
    }

    private static KafkaSink<String> kafkaProducer() {
        return KafkaSink.<String>builder()
                .setBootstrapServers("kafka-01:9092,kafka-02:9092,kafka-03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink-sink")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
