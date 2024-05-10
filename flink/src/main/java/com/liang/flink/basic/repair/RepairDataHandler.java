package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;


@Slf4j
public class RepairDataHandler implements Iterator<List<Map<String, Object>>> {
    private static final int QUERY_BATCH_SIZE = 1024;
    private static final int DIRECT_SCAN_COMPLETE_FLAG = -1;
    private final RepairTask task;
    private final String baseSql;
    private final JdbcTemplate jdbcTemplate;

    public RepairDataHandler(RepairTask task) {
        this.task = task;
        baseSql = String.format("select %s from %s where %s", task.getColumns(), task.getTableName(), task.getWhere());
        jdbcTemplate = new JdbcTemplate(task.getSourceName());
    }

    /**
     * `currentId` is unprocessed
     */
    @Override
    public boolean hasNext() {
        return task.getScanMode() == Direct ?
                task.getCurrentId() != DIRECT_SCAN_COMPLETE_FLAG : task.getCurrentId() <= task.getTargetId();
    }

    @Override
    public List<Map<String, Object>> next() {
        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        if (task.getScanMode() == TumblingWindow) {
            sqlBuilder.append(String.format(" and %s <= id and id < %s", task.getCurrentId(), task.getCurrentId() + QUERY_BATCH_SIZE));
        }
        return jdbcTemplate.queryForColumnMaps(sqlBuilder.toString());
    }
}
