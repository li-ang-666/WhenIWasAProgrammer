package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class RepairTask implements Serializable {
    protected String sourceName;
    protected String tableName;
    protected String columns = "*";
    protected String where = "1 = 1";
    protected ScanMode scanMode = ScanMode.Direct;

    public enum ScanMode implements Serializable {
        TumblingWindow, Direct
    }
}
