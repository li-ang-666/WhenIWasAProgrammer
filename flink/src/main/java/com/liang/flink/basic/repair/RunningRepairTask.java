package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;

@Data
public class RunningRepairTask {
    private RepairTask repairTask;
    private volatile long position;
    private volatile long count;
}
