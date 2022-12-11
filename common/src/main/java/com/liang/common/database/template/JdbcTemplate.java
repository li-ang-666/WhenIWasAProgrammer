package com.liang.common.database.template;

import com.liang.common.dto.ExecMode;
import com.liang.common.service.Timer;
import com.liang.common.database.cluster.JdbcPoolCluster;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class JdbcTemplate {
    private String name;

    public JdbcTemplate(String name) {
        this.name = name;
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        log.debug(name + " query: " + sql);
        ArrayList<T> list = new ArrayList<>();
        Timer timer = new Timer();
        try (Connection connection = JdbcPoolCluster.getConnectionByName(name)) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            list.add(resultSet.next() ? resultSetMapper.map(resultSet) : null);
        } catch (Exception e) {
            log.error("jdbc 查询异常, db: {}, sql: {}", name, sql,e);
            list.add(null);
        }
        log.debug(timer.getTimeMs() + " ms");
        return list.get(0);
    }

    public <T> List<T> queryForList(String sql, ResultSetMapper<T> resultSetMapper) {
        log.debug(name + " query: " + sql);
        ArrayList<T> list = new ArrayList<>();
        Timer timer = new Timer();
        try (Connection connection = JdbcPoolCluster.getConnectionByName(name)) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                list.add(resultSetMapper.map(resultSet));
            }
        } catch (Exception e) {
            log.error("jdbc 查询异常, db: {}, sql: {}", name, sql,e);
        }
        log.debug(timer.getTimeMs() + " ms");
        return list;
    }

    public void update(String sql, ExecMode mode) {
        log.info("{} {}: {}", name, mode, sql);
        if (mode.equals(ExecMode.TEST))
            return;
        Timer timer = new Timer();
        try (Connection connection = JdbcPoolCluster.getConnectionByName(name)) {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            log.error("jdbc update 异常, db: {}, sql: {}", name, sql,e);
        }
        log.debug(timer.getTimeMs() + " ms");
    }

    public void batchUpdate(List<String> sqls, ExecMode mode) {
        sqls.forEach(sql -> update(sql, mode));
    }
}

