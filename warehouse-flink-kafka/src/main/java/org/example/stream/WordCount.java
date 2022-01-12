package org.example.stream;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("all")
public class WordCount {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("127.0.0.1", 9000);
        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String f : split(s)) {
                    collector.collect(new Tuple2<>(f, 1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();
        environment.execute("StreamWordCount");
    }

    private static List<String> split(String text) {
        List<String> separator = Stream.of(",", ";", ".", "!").collect(Collectors.toList());
        if (StringUtils.isNotEmpty(text) && StringUtils.isNotEmpty(text.trim())) {
            text = text.trim().toLowerCase(Locale.ROOT);
            for (String separate : separator) {
                text = StringUtils.replace(text, separate, " ");
            }
            List<String> list = new ArrayList<>();
            String[] split = StringUtils.split(text, " ");
            for (String s : split) {
                list.add(s);
            }
            return list;
        }
        return Collections.EMPTY_LIST;
    }
}
