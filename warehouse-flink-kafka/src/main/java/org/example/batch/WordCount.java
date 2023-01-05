package org.example.batch;

import cn.hutool.core.util.ReUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("all")
public class WordCount {

    @SneakyThrows
    public static void main(String[] args) {
        String input = "/Users/chenkaikai/Downloads/ip.txt";
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = environment.readTextFile(input);
        dataSource.filter(text -> !ReUtil.isMatch("\\w+", text)).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String f : split(s)) {
                    collector.collect(new Tuple2<>(f, 1));
                }
            }
        }).groupBy(0).sum(1).print();
    }

    private static List<String> split(String text) {
        List<String> separator = Stream.of(",", ";", ".", "!", ":", "=", ">", "<", "%").collect(Collectors.toList());
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
