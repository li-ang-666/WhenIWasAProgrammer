package com.liang.flink.dto;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.concurrent.ConcurrentLinkedQueue;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SubRepairTask extends RepairTask {
    private final ConcurrentLinkedQueue<SingleCanalBinlog> pendingQueue = new ConcurrentLinkedQueue<>();
    private volatile long currentId;
    private long targetId;

    public SubRepairTask(RepairTask repairTask) {
        super(repairTask);
    }
}
