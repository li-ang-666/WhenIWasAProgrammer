package com.liang.flink.service;

import com.liang.common.dto.SubRepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;


@Slf4j
public class RepairTaskDataHandler {
    private final static int BATCH_SIZE = 200;
    private final JdbcTemplate jdbcTemplate;
    private final SubRepairTask task;
    private final String basicSql;
    private long windowBoundary;

    public RepairTaskDataHandler(SubRepairTask task) {
        this.jdbcTemplate = new JdbcTemplate(task.getSourceName());
        this.task = task;
        basicSql = String.format("select %s from %s where %s ",
                task.getColumns(), task.getTableName(), task.getWhere());
    }

    public List<Map<String, Object>> nextBatch() throws Exception {
        String sql = basicSql;
        if (task.getScanMode() == TumblingWindow) {
            windowBoundary = Math.min(task.getCurrentId() + BATCH_SIZE, task.getTargetId());
            sql += String.format(" and %s <= id and id < %s", task.getCurrentId(), windowBoundary);
        }
        return jdbcTemplate.queryForColumnMapList(sql);
    }

    public void commit() {
        task.setCurrentId(windowBoundary);
    }

    /**
     * used for:
     * do{}while()
     */
    public boolean hasMore() {
        if (task.getScanMode() == Direct)
            return false;
        return task.getCurrentId() < task.getTargetId();
    }
}
