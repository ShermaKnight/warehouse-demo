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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class HdfsJob {

    private final  static Logger logger = LoggerFactory.getLogger(HdfsJob.class);

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> kafkaSource = environment.fromSource(kafkaConsumer(), WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<HdfsBean> streamOperator = kafkaSource.map((MapFunction<String, HdfsBean>) s -> {
            try {
                if (StringUtils.isNoneEmpty(s)) {
                    logger.info(s);
                    return JSONObject.parseObject(s, HdfsBean.class);
                }
            } catch (Exception e) {
            }
            return null;
        }).filter((FilterFunction<HdfsBean>) hdfsBean -> Optional.ofNullable(hdfsBean).isPresent());
        streamOperator.map((MapFunction<HdfsBean, String>) hdfsBean -> JSONObject.toJSONString(hdfsBean)).sinkTo(kafkaProducer());
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
