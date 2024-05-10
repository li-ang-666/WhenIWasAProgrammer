package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.flink.dto.SubRepairTask;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

@Slf4j
@UtilityClass
public class TaskGenerator {
    private static final int DIRECT_SCAN_COMPLETE_FLAG = -1;

    public static SubRepairTask generateFrom(RepairTask task) {
        if (task.getScanMode() == RepairTask.ScanMode.Direct) {
            SubRepairTask subTask = new SubRepairTask(task);
            subTask.setCurrentId(0);
            subTask.setTargetId(DIRECT_SCAN_COMPLETE_FLAG);
            return subTask;
        }
        String sql = String.format("select min(id),max(id) from %s", task.getTableName());
        Tuple2<Long, Long> minAndMaxId = new JdbcTemplate(task.getSourceName())
                .queryForObject(sql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        if (minAndMaxId == null || minAndMaxId.f0 == null || minAndMaxId.f1 == null) {
            throw new RuntimeException(String.format("error while query min and max id, task: %s", task));
        }
        long minId = minAndMaxId.f0;
        long maxId = minAndMaxId.f1;
        SubRepairTask subTask = new SubRepairTask(task);
        subTask.setCurrentId(minId);
        subTask.setTargetId(maxId);
        //
        return subTask;
    }
}
