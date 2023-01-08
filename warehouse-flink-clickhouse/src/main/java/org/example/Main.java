package org.example;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://10.211.55.3:8123/");
        ClickHouseConnection connection = dataSource.getConnection();
        System.out.printf("初始化完成");
    }
}