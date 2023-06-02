package com.liang.common.dto;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SubRepairTask extends RepairTask {
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
