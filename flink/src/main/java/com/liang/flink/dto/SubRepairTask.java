package com.liang.flink.dto;

import com.liang.common.dto.config.RepairTask;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.concurrent.ConcurrentLinkedQueue;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SubRepairTask extends RepairTask {
    private final ConcurrentLinkedQueue<SingleCanalBinlog> pendingQueue = new ConcurrentLinkedQueue<>();
    private volatile long currentId;
    private long targetId;

    public SubRepairTask(RepairTask repairTask) {
        this.sourceName = repairTask.getSourceName();
        this.columns = repairTask.getColumns();
        this.tableName = repairTask.getTableName();
        this.where = repairTask.getWhere();
        this.scanMode = repairTask.getScanMode();
        this.checkpointUid = repairTask.getCheckpointUid();
    }
}
