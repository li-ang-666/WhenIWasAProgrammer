package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;

import java.io.Serializable;

@Data
public class RepairState implements Serializable {
    private volatile RepairTask repairTask;
    private volatile long maxParsedId = 0L;
}
