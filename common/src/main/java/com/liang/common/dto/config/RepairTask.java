package com.liang.common.dto.config;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class RepairTask implements Serializable {
    private Integer taskId;
    private String sourceName;
    private String tableName;
    private String columns = "*";
    private String where = "1 = 1";
    private ScanMode scanMode = ScanMode.TumblingWindow;
    // 游标 & 边界
    private volatile Long pivot = null;
    private Long upperBound = null;
    // 并发
    private Integer parallel = 2;
    // 所属下游并行度
    private List<Integer> channels = new ArrayList<>();

    public enum ScanMode implements Serializable {
        TumblingWindow, Direct
    }
}
