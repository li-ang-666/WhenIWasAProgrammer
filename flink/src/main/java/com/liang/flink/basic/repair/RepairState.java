package com.liang.flink.basic.repair;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class RepairState implements Serializable {
    private volatile RepairSplit repairSplit;
    private volatile long position;
    private volatile long count;
}
