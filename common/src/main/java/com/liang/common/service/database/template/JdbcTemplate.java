package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.service.database.holder.DruidHolder;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcTemplate {
    private final DruidDataSource druidDataSource;

    public JdbcTemplate(String name) {
        druidDataSource = DruidHolder.getDruid(name);
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            list.add(resultSet.next() ? resultSetMapper.map(resultSet) : null);
        } catch (Exception e) {
            log.error("JdbcTemplate Error, sql: {}", sql, e);
            list.add(null);
        }
        return list.get(0);
    }

    public <T> List<T> queryForList(String sql, ResultSetMapper<T> resultSetMapper) {
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                list.add(resultSetMapper.map(resultSet));
            }
        } catch (Exception e) {
            log.error("JdbcTemplate Error, sql: {}", sql, e);
        }
        return list;
    }

    public void batchUpdate(List<String> sqls) {
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
        } catch (Exception e) {
            for (String sql : sqls) {
                update(sql);
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (Exception ignore) {
                }
            }
        }
    }

    public void update(String sql) {
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            connection.setAutoCommit(true);
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            log.error("JdbcTemplate Error, sql: {}", sql, e);
        }
    }
}

