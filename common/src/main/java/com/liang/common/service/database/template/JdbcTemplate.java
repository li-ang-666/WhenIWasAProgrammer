package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.dto.ExecMode;
import com.liang.common.service.Timer;
import com.liang.common.service.database.holder.DruidHolder;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class JdbcTemplate {
    private final String name;

    public JdbcTemplate(String name) {
        this.name = name;
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        log.debug(name + " query: " + sql);
        ArrayList<T> list = new ArrayList<>();
        Timer timer = new Timer();
        try (DruidPooledConnection connection = DruidHolder.getConnectionByName(name)) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            list.add(resultSet.next() ? resultSetMapper.map(resultSet) : null);
        } catch (Exception e) {
            log.error("JdbcTemplate Error, db: {}, sql: {}", name, sql, e);
            list.add(null);
        }
        log.debug(timer.getTimeMs() + " ms");
        return list.get(0);
    }

    public <T> List<T> queryForList(String sql, ResultSetMapper<T> resultSetMapper) {
        log.debug(name + " query: " + sql);
        ArrayList<T> list = new ArrayList<>();
        Timer timer = new Timer();
        try (DruidPooledConnection connection = DruidHolder.getConnectionByName(name)) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                list.add(resultSetMapper.map(resultSet));
            }
        } catch (Exception e) {
            log.error("JdbcTemplate Error, db: {}, sql: {}", name, sql, e);
        }
        log.debug(timer.getTimeMs() + " ms");
        return list;
    }

    public void batchUpdate(List<String> sqls, ExecMode mode) {
        log.debug("{} {}: {}", name, mode, sqls);
        if (mode.equals(ExecMode.TEST))
            return;
        Timer timer = new Timer();
        try (DruidPooledConnection connection = DruidHolder.getConnectionByName(name)) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
        } catch (Exception e) {
            log.error("JdbcTemplate Error, db: {}, sql: {}", name, sqls, e);
            for (String sql : sqls) {
                update(sql, mode);
            }
        }
        log.debug(timer.getTimeMs() + " ms");
    }

    public void update(String sql, ExecMode mode) {
        log.debug("{} {}: {}", name, mode, sql);
        if (mode.equals(ExecMode.TEST))
            return;
        Timer timer = new Timer();
        try (DruidPooledConnection connection = DruidHolder.getConnectionByName(name)) {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            log.error("JdbcTemplate Error, db: {}, sql: {}", name, sql, e);
        }
        log.debug(timer.getTimeMs() + " ms");
    }
}

