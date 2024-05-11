package com.liang.flink.basic.repair;

import cn.hutool.core.util.SerializeUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@UtilityClass
public class TaskGenerator {
    @SneakyThrows
    public static void formatRepairTasks() {
        List<RepairTask> sourceRepairTasks = ConfigUtils.getConfig().getRepairTasks();
        ArrayList<RepairTask> resultRepairTasks = new ArrayList<>();
        // 遍历每个task
        for (RepairTask sourceRepairTask : sourceRepairTasks) {
            String sourceName = sourceRepairTask.getSourceName();
            String tableNameRegex = sourceRepairTask.getTableName();
            List<String> tableNames = new JdbcTemplate(sourceName)
                    .queryForList("show tables", rs -> rs.getString(1));
            // 遍历所有可能满足的表名
            for (String tableName : tableNames) {
                // 表名满足
                if (!tableName.matches(tableNameRegex)) {
                    continue;
                }
                // 克隆task, 指定正式表名, 指定id
                RepairTask resultRepairTask = SerializeUtil.clone(sourceRepairTask);
                resultRepairTask.setTableName(tableName);
                resultRepairTask.setTaskId(resultRepairTasks.size());
                // 设置位点 Direct模式
                if (resultRepairTask.getScanMode() == RepairTask.ScanMode.Direct) {
                    resultRepairTask.setPivot(0L);
                    resultRepairTask.setUpperBound(1L);
                }
                // 设置位点 TumblingWindow模式
                else {
                    String sql = String.format("select min(id), max(id) from %s", tableName);
                    Tuple2<Long, Long> minAndMaxId = new JdbcTemplate(sourceName)
                            .queryForObject(sql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
                    long minId = minAndMaxId.f0;
                    long maxId = minAndMaxId.f1;
                    if (resultRepairTask.getPivot() == null) {
                        resultRepairTask.setPivot(minId);
                    }
                    if (resultRepairTask.getUpperBound() == null) {
                        resultRepairTask.setUpperBound(maxId);
                    }
                }
                // 列入计划
                resultRepairTasks.add(resultRepairTask);
            }
        }
        log.info(StrUtil.repeat("=", 40));
        for (RepairTask resultRepairTask : resultRepairTasks) {
            log.info("RepairTask-{}: {}", resultRepairTask.getTaskId(), JsonUtils.toString(resultRepairTask));
        }
        log.info(StrUtil.repeat("=", 40));
        ConfigUtils.getConfig().setRepairTasks(resultRepairTasks);
    }
}
