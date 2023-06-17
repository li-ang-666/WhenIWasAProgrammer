package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.template.inner.ResultSetMapper;
import com.liang.common.service.database.template.inner.TemplateLogger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcTemplate {
    private final DruidDataSource pool;
    private final TemplateLogger logger;
    private final List<String> cache = new ArrayList<>();

    public JdbcTemplate(String name) {
        this(name, 500);
    }

    public JdbcTemplate(String name, int cacheTime) {
        pool = new DruidHolder().getPool(name);
        logger = new TemplateLogger(this.getClass().getSimpleName(), name);
        new Thread(new Sender(this, cacheTime)).start();
    }

    protected JdbcTemplate(DruidDataSource pool, TemplateLogger logger) {
        this.pool = pool;
        this.logger = logger;
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        logger.beforeExecute();
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            list.add(resultSet.next() ? resultSetMapper.map(resultSet) : null);
            logger.afterExecute("queryForObject", sql);
            return list.get(0);
        } catch (Exception e) {
            logger.ifError("queryForObject", sql, e);
            return null;
        }
    }

    public <T> List<T> queryForList(String sql, ResultSetMapper<T> resultSetMapper) {
        logger.beforeExecute();
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                list.add(resultSetMapper.map(resultSet));
            }
            logger.afterExecute("queryForList", sql);
            return list;
        } catch (Exception e) {
            logger.ifError("queryForList", sql, e);
            return list;
        }
    }

    public List<Map<String, Object>> queryForColumnMaps(String sql) {
        logger.beforeExecute();
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
            logger.afterExecute("queryForColumnMaps", sql);
            return result;
        } catch (Exception e) {
            logger.ifError("queryForColumnMaps", sql, e);
            return result;
        }
    }

    public void update(String... sqls) {
        if (sqls == null || sqls.length == 0) {
            return;
        }
        update(Arrays.asList(sqls));
    }

    public void update(List<String> sqls) {
        if (sqls == null || sqls.isEmpty()) {
            return;
        }
        cache.addAll(sqls);
    }

    public void updateImmediately(List<String> sqls) {
        logger.beforeExecute();
        try (DruidPooledConnection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
            logger.afterExecute("updateBatch", sqls);
        } catch (Exception e) {
            logger.ifError("updateBatch", sqls, e);
            for (String sql : sqls) {
                logger.beforeExecute();
                try (DruidPooledConnection connection = pool.getConnection()) {
                    connection.setAutoCommit(true);
                    connection.prepareStatement(sql).executeUpdate();
                    logger.afterExecute("updateSingle", sql);
                } catch (Exception ee) {
                    logger.ifError("updateSingle", sql, ee);
                }
            }
        }
    }

    private static class Sender implements Runnable {
        private final JdbcTemplate jdbcTemplate;
        private final int cacheTime;

        public Sender(JdbcTemplate jdbcTemplate, int cacheTime) {
            this.jdbcTemplate = jdbcTemplate;
            this.cacheTime = cacheTime;
        }

        @Override
        @SneakyThrows
        @SuppressWarnings("InfiniteLoopStatement")
        public void run() {
            while (true) {
                TimeUnit.MILLISECONDS.sleep(cacheTime);
                if (jdbcTemplate.cache.isEmpty()) {
                    continue;
                }
                List<String> copyCache;
                synchronized (jdbcTemplate.cache) {
                    copyCache = new ArrayList<>(jdbcTemplate.cache);
                    jdbcTemplate.cache.clear();
                }
                jdbcTemplate.updateImmediately(copyCache);
            }
        }
    }
}

