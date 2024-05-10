package com.liang.common.dto.config;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class RepairTask implements Serializable {
    private Integer taskId;
    private String sourceName;
    private String tableName;
    private String columns = "*";
    private String where = "1 = 1";

    private Long pivot = null;
    private Long upperBound = null;

    private ScanMode scanMode = ScanMode.TumblingWindow;

    public enum ScanMode implements Serializable {
        TumblingWindow, Direct
    }
}
