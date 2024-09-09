package com.liang.common.dto.config;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class RepairTask implements Serializable {
    private String sourceName;
    private String tableName;
    private String columns = "*";
    private String where = "1 = 1";
    private ScanMode scanMode = ScanMode.TumblingWindow;
}
