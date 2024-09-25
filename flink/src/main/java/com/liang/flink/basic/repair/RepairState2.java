package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Queue;

@Data
@AllArgsConstructor
public class RepairState2 implements Serializable {
    private Queue<RepairTask> pendingRepairTasks;
    private Map<RepairTask, Long> finishedRepairTasks;
    private RepairTask runningRepairTask;
    private volatile long position;
    private volatile long count;


}

