package com.liang.common.dto.config;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class RepairTask implements Serializable {
    protected String taskId;
    protected String sourceName;
    protected String tableName;
    protected String columns = "*";
    protected String where = "1 = 1";
    protected ScanMode scanMode = ScanMode.Direct;

    public enum ScanMode implements Serializable {
        TumblingWindow, Direct
    }
}
