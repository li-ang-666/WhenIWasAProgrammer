package com.liang.flink.basic;

import com.liang.common.dto.SubRepairTask;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.service.TaskGenerator;

import java.util.ArrayList;
import java.util.List;

public class FlinkRepairSourceFactory {
    private FlinkRepairSourceFactory() {
    }

    public static List<FlinkRepairSource> create() {
        List<FlinkRepairSource> result = new ArrayList<>();
        List<RepairTask> repairTasks = ConfigUtils.getConfig().getRepairTasks();
        for (RepairTask repairTask : repairTasks) {
            SubRepairTask subRepairTask = TaskGenerator.generateFrom(repairTask);
            if (subRepairTask != null)
                result.add(new FlinkRepairSource(ConfigUtils.getConfig(), subRepairTask));
        }
        return result;
    }
}
