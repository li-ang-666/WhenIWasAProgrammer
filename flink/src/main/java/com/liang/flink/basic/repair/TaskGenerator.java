package com.liang.flink.basic.repair;

import cn.hutool.core.util.SerializeUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@UtilityClass
public class TaskGenerator {
    private static final List<RepairTask> RESULT_REPAIR_TASKS = new ArrayList<>();
    private static final AtomicInteger TASK_ID = new AtomicInteger(0);
    private static final AtomicInteger CHANNEL = new AtomicInteger(0);

    @SneakyThrows
    public static void formatRepairTasks() {
        // 遍历每个task
        for (RepairTask sourceRepairTask : ConfigUtils.getConfig().getRepairTasks()) {
            String sourceName = sourceRepairTask.getSourceName();
            String tableNameRegex = sourceRepairTask.getTableName();
            List<String> tableNames = new JdbcTemplate(sourceName)
                    .queryForList("SHOW TABLES", rs -> rs.getString(1));
            // 遍历所有可能满足的表名
            for (String tableName : tableNames) {
                // 表名满足
                if (!tableName.matches(tableNameRegex)) {
                    continue;
                }
                // 克隆task, 指定正式表名
                RepairTask resultRepairTask = SerializeUtil.clone(sourceRepairTask);
                resultRepairTask.setTableName(tableName);
                // Direct模式
                if (resultRepairTask.getScanMode() == RepairTask.ScanMode.Direct) {
                    RESULT_REPAIR_TASKS.add(getFormattedDirectRepairTasks(resultRepairTask));
                }
                // TumblingWindow模式
                else if (resultRepairTask.getScanMode() == RepairTask.ScanMode.TumblingWindow) {
                    RESULT_REPAIR_TASKS.add(getFormattedTumblingRepairTasks(resultRepairTask));
                }
            }
        }
        log.info(StrUtil.repeat("=", 50));
        for (RepairTask resultRepairTask : RESULT_REPAIR_TASKS) {
            log.info("RepairTask-{}: {}", resultRepairTask.getTaskId(), JsonUtils.toString(resultRepairTask));
        }
        log.info(StrUtil.repeat("=", 50));
        ConfigUtils.getConfig().setRepairTasks(RESULT_REPAIR_TASKS);
    }

    private static RepairTask getFormattedDirectRepairTasks(RepairTask repairTask) {
        // 边界
        repairTask.setPivot(0L);
        repairTask.setUpperBound(1L);
        // id
        repairTask.setTaskId(TASK_ID.getAndIncrement());
        // channels
        repairTask.getChannels().add(CHANNEL.getAndIncrement());
        return repairTask;
    }

    private static RepairTask getFormattedTumblingRepairTasks(RepairTask repairTask) {
        String sourceName = repairTask.getSourceName();
        String tableName = repairTask.getTableName();
        Long pivot = repairTask.getPivot();
        Long upperBound = repairTask.getUpperBound();
        String sql = new SQL()
                .SELECT("min(id)", "max(id) + 1")
                .FROM(tableName)
                .toString();
        Tuple2<Long, Long> minAndMaxId = new JdbcTemplate(sourceName)
                .queryForObject(sql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        long minId = pivot != null ? pivot : minAndMaxId.f0;
        long maxId = upperBound != null ? upperBound : minAndMaxId.f1;
        // 边界
        repairTask.setPivot(minId);
        repairTask.setUpperBound(maxId);
        // id
        repairTask.setTaskId(TASK_ID.getAndIncrement());
        // channels
        for (int i = 0; i < repairTask.getParallel(); i++) {
            repairTask.getChannels().add(CHANNEL.getAndIncrement());
        }
        return repairTask;
    }
}
