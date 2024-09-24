package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RepairGenerator {
    public List<RepairSplit> generate() {
        List<RepairSplit> repairSplits = new ArrayList<>();
        // 遍历每个task
        for (RepairTask repairTask : ConfigUtils.getConfig().getRepairTasks()) {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            List<String> tableNames = jdbcTemplate.queryForList("SHOW TABLES",
                    rs -> rs.getString(1));
            // 遍历所有可能满足的表名
            for (String tableName : tableNames) {
                // 表名满足
                if (!tableName.matches(repairTask.getTableName())) {
                    continue;
                }
                for (int i = 0; i < repairTask.getSplitNum(); i++) {
                    // 拼装sql
                    SQL sql = new SQL()
                            .SELECT(repairTask.getColumns())
                            .FROM(repairTask.getTableName())
                            .WHERE(repairTask.getWhere())
                            .ORDER_BY("id ASC");
                    if (repairTask.getSplitNum() > 1) {
                        sql.WHERE("id % " + repairTask.getSplitNum() + " = " + i);
                    }
                    // 保存
                    repairSplits.add(new RepairSplit(repairTask.getSourceName(), repairTask.getTableName(), sql));
                }
            }
        }
        return repairSplits;
    }
}
