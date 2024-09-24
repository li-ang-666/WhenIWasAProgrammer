package com.liang.flink.basic.repair;

import com.liang.common.service.SQL;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class RepairSplit implements Serializable {
    private String sourceName;
    private SQL sql;
}
