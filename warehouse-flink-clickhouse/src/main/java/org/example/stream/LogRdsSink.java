package org.example.stream;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogRdsSink extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.connection = LogDataSource.getConnection();
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
        String sql = "insert into ch_local values (?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value.getString("ts"));
        preparedStatement.setString(2, value.getString("uid"));
        preparedStatement.setString(3, value.getString("biz"));
        preparedStatement.executeUpdate();
    }

}
