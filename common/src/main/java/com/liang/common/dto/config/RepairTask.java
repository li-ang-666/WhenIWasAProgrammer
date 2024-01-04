package com.liang.common.dto.config;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class RepairTask implements Serializable {
    protected volatile String taskId;
    protected volatile String sourceName;
    protected volatile String tableName;
    protected volatile String columns = "*";
    protected volatile String where = "1 = 1";
    protected volatile ScanMode scanMode = ScanMode.Direct;

    public enum ScanMode implements Serializable {
        TumblingWindow, Direct
    }
}
