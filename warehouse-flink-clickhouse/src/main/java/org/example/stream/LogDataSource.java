package org.example.stream;

import com.clickhouse.jdbc.ClickHouseDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class LogDataSource {

    private static ClickHouseDataSource dataSource;

    static {
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            Properties properties = new Properties();
            dataSource = new ClickHouseDataSource("jdbc:clickhouse://clickhouse-server-06:8123/", properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private LogDataSource() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
