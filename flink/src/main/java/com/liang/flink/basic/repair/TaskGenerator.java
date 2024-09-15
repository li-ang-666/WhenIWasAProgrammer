package com.liang.flink.basic.repair;

import cn.hutool.core.util.SerializeUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@UtilityClass
public class TaskGenerator {
    private static final List<RepairTask> RESULT_REPAIR_TASKS = new ArrayList<>();

    public static void formatRepairTasks() {
        // 本地串联执行flink任务,只进行一次格式化
        if (ConfigUtils.getConfig().getRepairTasks() == RESULT_REPAIR_TASKS) {
            return;
        }
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
                RESULT_REPAIR_TASKS.add(resultRepairTask);
            }
        }
        log.info(StrUtil.repeat("=", 50));
        for (RepairTask resultRepairTask : RESULT_REPAIR_TASKS) {
            log.info("{}", JsonUtils.toString(resultRepairTask));
        }
        log.info(StrUtil.repeat("=", 50));
        ConfigUtils.getConfig().setRepairTasks(RESULT_REPAIR_TASKS);
    }
}
