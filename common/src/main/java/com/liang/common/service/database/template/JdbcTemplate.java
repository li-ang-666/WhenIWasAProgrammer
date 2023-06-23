package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.service.ListCache;
import com.liang.common.service.Logging;
import com.liang.common.service.database.holder.DruidHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcTemplate extends ListCache<String> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 500;
    private final static int DEFAULT_CACHE_RECORDS = 1024;
    private final DruidDataSource pool;
    private final Logging logging;

    public JdbcTemplate(String name) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS);
        pool = new DruidHolder().getPool(name);
        logging = new Logging(this.getClass().getSimpleName(), name);
    }

    // just for MemJdbcTemplate
    protected JdbcTemplate(DruidDataSource pool, Logging logging) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS);
        this.pool = pool;
        this.logging = logging;
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
                    columnMap.put(metaData.getColumnName(i), resultSet.getString(i));
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

    @Override
    public void updateImmediately(String... sqls) {
        super.updateImmediately(sqls);
    }

    @Override
    public synchronized void updateImmediately(List<String> sqls) {
        if (sqls == null || sqls.isEmpty()) {
            return;
        }
        logging.beforeExecute();
        try (DruidPooledConnection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
            logging.afterExecute("updateBatch", sqls);
        } catch (Exception e) {
            logging.ifError("updateBatch", sqls, e);
            for (String sql : sqls) {
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (Exception ignore) {
                }
                logging.beforeExecute();
                try (DruidPooledConnection connection = pool.getConnection()) {
                    connection.setAutoCommit(true);
                    connection.prepareStatement(sql).executeUpdate();
                    logging.afterExecute("updateSingle", sql);
                } catch (Exception ee) {
                    logging.ifError("updateSingle", sql, ee);
                }
            }
        }
    }

    @FunctionalInterface
    public interface ResultSetMapper<T> extends Serializable {
        T map(ResultSet rs) throws Exception;
    }
}

