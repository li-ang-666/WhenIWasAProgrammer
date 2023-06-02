package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.service.database.holder.DruidHolder;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcTemplate {
    private final String name;
    private final DruidDataSource druidDataSource;

    public JdbcTemplate(String name) {
        this.name = name;
        druidDataSource = DruidHolder.getDruid(name);
    }

    private void preExecute(Object sqlOrBatch) {
        log.debug("jdbcTemplate {} execute: {}", name, sqlOrBatch);
    }

    private void whenError(Object sqlOrBatch, Exception e) {
        log.error("jdbcTemplate {} execute error: {}", name, sqlOrBatch, e);
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        preExecute(sql);
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            list.add(resultSet.next() ? resultSetMapper.map(resultSet) : null);
        } catch (Exception e) {
            whenError(sql, e);
            list.add(null);
        }
        return list.get(0);
    }

    public <T> List<T> queryForList(String sql, ResultSetMapper<T> resultSetMapper) {
        preExecute(sql);
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                list.add(resultSetMapper.map(resultSet));
            }
        } catch (Exception e) {
            whenError(sql, e);
        }
        return list;
    }

    public List<Map<String, Object>> queryForColumnMapList(String sql) {
        preExecute(sql);
        List<Map<String, Object>> result = new ArrayList<>();
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                HashMap<String, Object> columnMap = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    columnMap.put(metaData.getColumnName(i), resultSet.getString(i));
                }
                result.add(columnMap);
            }
        } catch (Exception e) {
            whenError(sql, e);
        }
        return result;
    }

    public void batchUpdate(List<String> sqls) {
        preExecute(sqls);
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
        } catch (Exception e) {
            whenError(sqls, e);
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
        preExecute(sql);
        try (DruidPooledConnection connection = druidDataSource.getConnection()) {
            connection.setAutoCommit(true);
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            whenError(sql, e);
        }
    }
}

