package com.liang.flink.basic;

import com.liang.flink.dto.SubRepairTask;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.service.TaskGenerator;

import java.util.ArrayList;
import java.util.List;

public class RepairSourceFactory {
    private RepairSourceFactory() {
    }

    public static List<RepairSource> create() {
        List<RepairSource> result = new ArrayList<>();
        List<RepairTask> repairTasks = ConfigUtils.getConfig().getRepairTasks();
        for (RepairTask repairTask : repairTasks) {
            SubRepairTask subRepairTask = TaskGenerator.generateFrom(repairTask);
            if (subRepairTask != null)
                result.add(new RepairSource(ConfigUtils.getConfig(), subRepairTask));
        }
        return result;
    }
}
