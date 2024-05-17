package com.liang.flink.basic.repair;

import lombok.Data;

@Data
public class RepairSplit {
    private final Integer taskId;
    private final String sourceName;
    private final String tableName;
    private final Integer channel;
    private final String sql;
}
