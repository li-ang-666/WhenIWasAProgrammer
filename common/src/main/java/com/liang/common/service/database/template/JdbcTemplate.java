package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.service.AbstractCache;
import com.liang.common.service.Logging;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.util.DorisBitmapUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcTemplate extends AbstractCache<String, String> {
    private final static int BUFFER_MAX_MB = 16; // 1kb/条 x 16000条
    private final static int DEFAULT_CACHE_MILLISECONDS = 3000;
    private final static int DEFAULT_CACHE_RECORDS = 128;
    private final static String BITMAP_COLUMN_NAME = "bitmap";
    private final DruidDataSource pool;
    private final Logging logging;

    public JdbcTemplate(String name) {
        super(BUFFER_MAX_MB, DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, sql -> "");
        pool = new DruidHolder().getPool(name);
        logging = new Logging(this.getClass().getSimpleName(), name);
    }

    @Override
    @SneakyThrows(InterruptedException.class)
    protected void updateImmediately(String ignore, Queue<String> sqls) {
        boolean getException = false;
        logging.beforeExecute();
        try (DruidPooledConnection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
            logging.afterExecute("updateBatch", sqls, sqls.size() + "条");
        } catch (Exception e) {
            // 归还的时候, DruidDataSource.recycle 会自动 rollback 一次
            // 批量更新报错不打印明细, 单条执行报错再打印具体SQL
            logging.ifError("updateBatch", sqls.size() + "条", e);
            getException = true;
        }
        if (getException) {
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
                        String methodArg = i == 1 ? "/* Exception: " + ee.getMessage() + " */" + " " + failedLogPrefix + sql : failedLogPrefix + sql;
                        logging.ifError("updateSingle", methodArg, ee);
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

