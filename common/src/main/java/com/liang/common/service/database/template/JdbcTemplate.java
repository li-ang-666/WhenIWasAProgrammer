package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.template.inner.ResultSetMapper;
import com.liang.common.service.database.template.inner.TemplateLogger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcTemplate {
    private final static int DEFAULT_CACHE_TIME = 500;
    private final static int DEFAULT_CACHE_SIZE = 1024;
    private final DruidDataSource pool;
    private final TemplateLogger logger;
    private final List<String> cache = new ArrayList<>();

    private volatile boolean enableCache = false;

    public JdbcTemplate(String name) {
        pool = new DruidHolder().getPool(name);
        logger = new TemplateLogger(this.getClass().getSimpleName(), name);
    }

    // just for MemJdbcTemplate
    protected JdbcTemplate(DruidDataSource pool, TemplateLogger logger) {
        this.pool = pool;
        this.logger = logger;
    }

    public JdbcTemplate enableCache() {
        return enableCache(DEFAULT_CACHE_TIME);
    }

    public JdbcTemplate enableCache(int cacheTime) {
        if (!enableCache) {
            enableCache = true;
            new Thread(new Sender(this, cacheTime)).start();
        }
        return this;
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        if (StringUtils.isBlank(sql)) {
            return null;
        }
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
        ArrayList<T> list = new ArrayList<>();
        if (StringUtils.isBlank(sql)) {
            return list;
        }
        logger.beforeExecute();
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
        synchronized (cache) {
            cache.addAll(sqls);
            if (cache.size() >= DEFAULT_CACHE_SIZE) {
                updateImmediately(cache);
                cache.clear();
            }
        }
        if (!enableCache) {
            updateImmediately(cache);
            cache.clear();
        }
    }

    public void updateImmediately(String... sqls) {
        if (sqls == null || sqls.length == 0) {
            return;
        }
        updateImmediately(Arrays.asList(sqls));
    }

    public synchronized void updateImmediately(List<String> sqls) {
        if (sqls == null || sqls.isEmpty()) {
            return;
        }
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
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (Exception ignore) {
                }
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
                    if (jdbcTemplate.cache.isEmpty()) {
                        continue;
                    }
                    copyCache = new ArrayList<>(jdbcTemplate.cache);
                    jdbcTemplate.cache.clear();
                }
                jdbcTemplate.updateImmediately(copyCache);
            }
        }
    }
}

