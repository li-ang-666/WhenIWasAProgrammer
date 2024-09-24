package com.liang.flink.basic.repair;

import com.liang.common.service.SQL;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RepairSplit {
    private String sourceName;
    private String tableName;
    private SQL sql;
}
