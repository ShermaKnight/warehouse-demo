package org.example.controller;

import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.example.vo.ExecuteRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

@RestController
@RequestMapping("/hive")
public class HiveController {

    @Resource
    private JdbcTemplate hiveTemplate;

    @Resource
    private DataSource dataSource;

    @SneakyThrows
    @PostMapping("/execute/query")
    public Object executeQuery(@RequestBody ExecuteRequest request) {
        String sql = request.getSql();
        if (StringUtils.isEmpty(sql) || !StringUtils.contains(sql.toLowerCase(Locale.ROOT), "select")) {
            throw new RuntimeException("sql invalid");
        }
        List<HashMap<String, String>> cache = new ArrayList<>();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(request.getSql());
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            HashMap<String, String> innerCache = new HashMap<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String column = metaData.getColumnName(i);
                innerCache.put(column, resultSet.getString(column));
            }
            cache.add(innerCache);
        }
        statement.close();
        connection.close();
        return cache;
    }
}
