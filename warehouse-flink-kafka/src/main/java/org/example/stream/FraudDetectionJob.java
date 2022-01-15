package org.example.stream;

import lombok.SneakyThrows;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStateBackend(new FsStateBackend("hdfs://192.168.71.128:8020/flink/checkpoints"));
        CheckpointConfig config = environment.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(60000);

        DataStream<Transaction> transactions = environment.addSource(new TransactionSource()).name("transactions");
        DataStream<Alert> alerts = transactions.keyBy(Transaction::getAccountId).process(new FraudDetection()).name("fraud-detector");
        alerts.addSink(new AlertSink()).name("send-alerts");
        environment.execute("Fraud Detection");
    }
}
