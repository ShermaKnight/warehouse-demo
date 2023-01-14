package org.example.stream;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

public class LogSinkFunction extends RichSinkFunction<LogBean> {

    private static DruidPooledConnection connection;
    private final static Logger logger = LoggerFactory.getLogger(LogSinkFunction.class);

    static {
        try {
            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
            dataSource.setUrl("jdbc:phoenix:10.211.55.3:2181");
            dataSource.setTestOnBorrow(false);
            dataSource.setTestOnReturn(false);
            dataSource.setTestWhileIdle(true);
            dataSource.setTimeBetweenEvictionRunsMillis(60000);
            dataSource.setMaxActive(20);
            dataSource.setInitialSize(5);
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            logger.error("Initialize datasource failed. ", e);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append(" create  table  if  not  exists wps.tb_log (");
            sql.append("id varchar primary key," +
                    "common.ch varchar," +
                    "common.md varchar," +
                    "common.mid varchar," +
                    "common.os varchar," +
                    "page.during_time varchar," +
                    "page.page_id varchar," +
                    "displays varchar," +
                    "ts varchar");
            sql.append(" )");
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            logger.error("Execute create table failed. ", e);
        } finally {
            if (Optional.ofNullable(preparedStatement).isPresent()) {
                preparedStatement.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(LogBean value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("  upsert into \"wps.tb_log\"  values(");
            sql.append("'").append(System.currentTimeMillis()).append("'").append(",");
            sql.append("'").append(value.getCommon().getCh()).append("'").append(",");
            sql.append("'").append(value.getCommon().getMd()).append("'").append(",");
            sql.append("'").append(value.getCommon().getMd()).append("'").append(",");
            sql.append("'").append(value.getCommon().getOs()).append("'").append(",");
            sql.append("'").append(value.getPage().getDuring_time()).append("'").append(",");
            sql.append("'").append(value.getPage().getPage_id()).append("'").append(",");
            sql.append("'").append(value.getDisplays()).append("'").append(",");
            sql.append("'").append(value.getTs()).append("'");
            sql.append(")");
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            logger.error("Execute upsert table failed. ", e);
        } finally {
            if (Optional.ofNullable(preparedStatement).isPresent()) {
                preparedStatement.close();
            }
        }
    }
}
