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
    public final DruidDataSource pool;
    private final TemplateLogger logger;

    public JdbcTemplate(String name) {
        pool = new DruidHolder().getPool(name);
        logger = new TemplateLogger(this.getClass().getSimpleName(), name);
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        logger.beforeExecute("queryForObject", sql);
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            list.add(resultSet.next() ? resultSetMapper.map(resultSet) : null);
        } catch (Exception e) {
            logger.ifError("queryForObject", sql, e);
            list.add(null);
        }
        logger.afterExecute("queryForObject", sql);
        return list.get(0);
    }

    public <T> List<T> queryForList(String sql, ResultSetMapper<T> resultSetMapper) {
        logger.beforeExecute("queryForList", sql);
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                list.add(resultSetMapper.map(resultSet));
            }
        } catch (Exception e) {
            logger.ifError("queryForList", sql, e);
        }
        logger.afterExecute("queryForList", sql);
        return list;
    }

    public List<Map<String, Object>> queryForColumnMaps(String sql) {
        logger.beforeExecute("queryForColumnMaps", sql);
        List<Map<String, Object>> result = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
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
            logger.ifError("queryForColumnMaps", sql, e);
        }
        logger.afterExecute("queryForColumnMaps", sql);
        return result;
    }

    public void batchUpdate(List<String> sqls) {
        logger.beforeExecute("batchUpdate", sqls);
        try (DruidPooledConnection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
        } catch (Exception e) {
            logger.ifError("batchUpdate", sqls, e);
            for (String sql : sqls) {
                update(sql);
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (Exception ignore) {
                }
            }
        }
        logger.afterExecute("batchUpdate", sqls);
    }

    public void update(String sql) {
        logger.beforeExecute("update", sql);
        try (DruidPooledConnection connection = pool.getConnection()) {
            connection.setAutoCommit(true);
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            logger.ifError("update", sql, e);
        }
        logger.afterExecute("update", sql);
    }
}

