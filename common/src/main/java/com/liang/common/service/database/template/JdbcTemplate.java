package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.service.AbstractCache;
import com.liang.common.service.Logging;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.util.DorisBitmapUtils;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcTemplate extends AbstractCache<Object, String> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 500;
    private final static int DEFAULT_CACHE_RECORDS = 1024;
    private final static String BITMAP_COLUMN_NAME = "bitmap";
    private final static String DEAD_LOCK_MESSAGE = "Deadlock found when trying to get lock; try restarting transaction";
    private final DruidDataSource pool;
    private final Logging logging;

    public JdbcTemplate(String name) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, sql -> null);
        pool = new DruidHolder().getPool(name);
        logging = new Logging(this.getClass().getSimpleName(), name);
    }

    // just for MemJdbcTemplate
    protected JdbcTemplate(DruidDataSource pool, Logging logging) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, sql -> null);
        this.pool = pool;
        this.logging = logging;
    }

    @Override
    @Synchronized
    @SneakyThrows(InterruptedException.class)
    protected void updateImmediately(Object ignore, List<String> sqls) {
        logging.beforeExecute();
        try (DruidPooledConnection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
            Object methodArg = sqls.size() > 100 ? sqls.size() + "条" : sqls;
            logging.afterExecute("updateBatch", methodArg);
        } catch (Exception e) {
            // 归还的时候, DruidDataSource.recycle 会自动 rollback 一次
            if (DEAD_LOCK_MESSAGE.equals(e.getMessage())) {
                logging.ifError("updateBatch", sqls, new SQLException(DEAD_LOCK_MESSAGE));
            } else {
                logging.ifError("updateBatch", sqls, e);
            }
            for (String sql : sqls) {
                TimeUnit.MILLISECONDS.sleep(50);
                int i = 3;
                while (i > 0) {
                    String failedLogPrefix = "/* 第" + (4 - i) + "次重试 */";
                    logging.beforeExecute();
                    try (DruidPooledConnection connection = pool.getConnection()) {
                        connection.setAutoCommit(true);
                        connection.prepareStatement(sql).executeUpdate();
                        logging.afterExecute("updateSingle", failedLogPrefix + sql);
                        i = 0;
                    } catch (Exception ee) {
                        logging.ifError("updateSingle", failedLogPrefix + sql, ee);
                        i--;
                        TimeUnit.MILLISECONDS.sleep(50);
                    }
                }
            }
        }
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        if (StringUtils.isBlank(sql)) {
            return null;
        }
        logging.beforeExecute();
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            list.add(resultSet.next() ? resultSetMapper.map(resultSet) : null);
            logging.afterExecute("queryForObject", sql);
            return list.get(0);
        } catch (Exception e) {
            logging.ifError("queryForObject", sql, e);
            return null;
        }
    }

    public <T> List<T> queryForList(String sql, ResultSetMapper<T> resultSetMapper) {
        ArrayList<T> list = new ArrayList<>();
        if (StringUtils.isBlank(sql)) {
            return list;
        }
        logging.beforeExecute();
        try (DruidPooledConnection connection = pool.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                list.add(resultSetMapper.map(resultSet));
            }
            logging.afterExecute("queryForList", sql);
            return list;
        } catch (Exception e) {
            logging.ifError("queryForList", sql, e);
            return list;
        }
    }

    public List<Map<String, Object>> queryForColumnMaps(String sql) {
        logging.beforeExecute();
        List<Map<String, Object>> result = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                HashMap<String, Object> columnMap = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = BITMAP_COLUMN_NAME.equals(columnName) ?
                            DorisBitmapUtils.parseBinary(resultSet.getBytes(i)) : resultSet.getString(i);
                    columnMap.put(columnName, columnValue);
                }
                result.add(columnMap);
            }
            logging.afterExecute("queryForColumnMaps", sql);
            return result;
        } catch (Exception e) {
            logging.ifError("queryForColumnMaps", sql, e);
            return result;
        }
    }

    @FunctionalInterface
    public interface ResultSetMapper<T> extends Serializable {
        T map(ResultSet rs) throws Exception;
    }
}

