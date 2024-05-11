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
import java.util.Collections;
import java.util.List;

@Slf4j
@UtilityClass
public class TaskGenerator {
    @SneakyThrows
    public static void formatRepairTasks() {
        ArrayList<RepairTask> resultRepairTasks = new ArrayList<>();
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
                    resultRepairTasks.addAll(getBoundedDirectRepairTasks(resultRepairTask));
                }
                // TumblingWindow模式
                else if (resultRepairTask.getScanMode() == RepairTask.ScanMode.TumblingWindow) {
                    resultRepairTasks.addAll(getBoundedTumblingRepairTasks(resultRepairTask));
                }
            }
        }
        log.info(StrUtil.repeat("=", 50));
        for (int i = 0; i < resultRepairTasks.size(); i++) {
            RepairTask resultRepairTask = resultRepairTasks.get(i);
            resultRepairTask.setTaskId(i);
            log.info("RepairTask-{}: {}", resultRepairTask.getTaskId(), JsonUtils.toString(resultRepairTask));
        }
        log.info(StrUtil.repeat("=", 50));
        ConfigUtils.getConfig().setRepairTasks(resultRepairTasks);
    }

    private static List<RepairTask> getBoundedDirectRepairTasks(RepairTask repairTask) {
        repairTask.setPivot(0L);
        repairTask.setUpperBound(1L);
        return Collections.singletonList(repairTask);
    }

    private static List<RepairTask> getBoundedTumblingRepairTasks(RepairTask repairTask) {
        String sourceName = repairTask.getSourceName();
        String tableName = repairTask.getTableName();
        Long pivot = repairTask.getPivot();
        Long upperBound = repairTask.getUpperBound();
        String sql = new SQL()
                .SELECT("min(id)", "max(id)")
                .FROM(tableName)
                .toString();
        Tuple2<Long, Long> minAndMaxId = new JdbcTemplate(sourceName)
                .queryForObject(sql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        long minId = pivot != null ? pivot : minAndMaxId.f0;
        long maxId = upperBound != null ? upperBound : minAndMaxId.f1;
        // 多并行度
        int parallel = repairTask.getParallel();
        long lag = maxId - minId;
        long interval = lag / parallel + 1;
        ArrayList<RepairTask> boundedTumblingRepairTasks = new ArrayList<>();
        // 切分边界
        for (int i = 0; i < parallel; i++) {
            long start = minId + interval * i;
            long end = start + interval;
            RepairTask parallelResultRepairTask = SerializeUtil.clone(repairTask);
            parallelResultRepairTask.setPivot(start);
            parallelResultRepairTask.setUpperBound(end);
            boundedTumblingRepairTasks.add(parallelResultRepairTask);
        }
        return boundedTumblingRepairTasks;
    }
}
