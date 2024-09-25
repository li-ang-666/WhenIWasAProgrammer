package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class RepairSplit implements Serializable {
    private RepairTask repairTask;
    private long minId;
    private long maxId;
}
