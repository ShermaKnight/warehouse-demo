package org.example.event;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventRdsSink extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.connection = EventDataSource.getConnection();
    }

    @Override
    public void close() throws Exception {
        if (Optional.ofNullable(connection).isPresent()) {
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("insert into event($1) values($2)");
        List<String> keys = Stream.of("id").collect(Collectors.toList());
        List<String> values = Stream.of("NULL").collect(Collectors.toList());
        for (String key : value.keySet()) {
            keys.add(key);
            values.add("'" + value.get(key).toString() + "'");
        }
        String sql = StringUtils.replace(
                StringUtils.replace(builder.toString(), "$1", keys.stream().collect(Collectors.joining(","))), "$2", values.stream().collect(Collectors.joining(",")));
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.executeUpdate();
    }

}
