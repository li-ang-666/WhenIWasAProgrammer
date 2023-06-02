package com.liang.flink.service;

import com.liang.common.dto.SubRepairTask;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

@Slf4j
public class TaskGenerator {
    private TaskGenerator() {
    }

    public static SubRepairTask generateFrom(RepairTask task) {
        if (task.getScanMode() == RepairTask.ScanMode.Direct) {
            return new SubRepairTask(task);
        }
        JdbcTemplate jdbcTemplate = new JdbcTemplate(task.getSourceName());
        String sql = String.format("select min(id),max(id) from %s", task.getTableName());
        Tuple2<Long, Long> minAndMaxId = jdbcTemplate.queryForObject(sql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        if (minAndMaxId == null || minAndMaxId.f0 == null || minAndMaxId.f1 == null) {
            log.error("task: {} has none or error data in source", task);
            return null;
        }
        long minId = minAndMaxId.f0;
        long maxId = minAndMaxId.f1;
        SubRepairTask subTask = new SubRepairTask(task);
        subTask.setCurrentId(minId);
        subTask.setTargetId(maxId);
        return subTask;
    }
}
