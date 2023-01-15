package org.example.stream;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public class HdfsJob {

    private final static Logger logger = LoggerFactory.getLogger(HdfsJob.class);

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStateBackend(new FsStateBackend("hdfs://namenode:8020/flink/checkpoints/"));
        CheckpointConfig config = environment.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(60000);

        DataStreamSource<String> kafkaSource = environment.fromSource(kafkaConsumer(), WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaSource.map((MapFunction<String, HdfsBean>) s -> {
                    try {
                        if (StringUtils.isNoneEmpty(s)) {
                            logger.info(s);
                            return JSONObject.parseObject(s, HdfsBean.class);
                        }
                    } catch (Exception e) {
                    }
                    return null;
                }).filter((FilterFunction<HdfsBean>) hdfsBean -> Optional.ofNullable(hdfsBean).isPresent())
                .map((MapFunction<HdfsBean, String>) hdfsBean -> JSONObject.toJSONString(hdfsBean))
                .name("Data Check")
                .sinkTo(kafkaProducer()).name("Kafka Sink").setParallelism(3);

        kafkaSource.map((MapFunction<String, HdfsBean>) s -> {
                    try {
                        if (StringUtils.isNoneEmpty(s)) {
                            logger.info(s);
                            return JSONObject.parseObject(s, HdfsBean.class);
                        }
                    } catch (Exception e) {
                    }
                    return null;
                }).filter((FilterFunction<HdfsBean>) hdfsBean -> Optional.ofNullable(hdfsBean).isPresent())
                .map((MapFunction<HdfsBean, String>) hdfsBean -> JSONObject.toJSONString(hdfsBean))
                .name("Data Check")
                .sinkTo(fileSink()).name("HDFS Sink").setParallelism(2);
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

    private static FileSink<String> fileSink() {
        return FileSink
                .forRowFormat(new Path("hdfs://namenode:8020/hive/warehouse/vehicle_ods.db/"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new BucketAssigner<String, String>() {
                    @Override
                    public String getBucketId(String s, Context context) {
                        String currentDate = DateUtil.now().split(" ")[0].replace("-", "");
                        String bucketId = "dt=" + currentDate;
                        return bucketId;
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build())
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("vehicle")
                                .withPartSuffix(".data")
                                .build()
                )
                .build();
    }
}
