package com.liang.flink.basic.repair;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.Partitioner;

@Slf4j
public class RepairPartitioner implements Partitioner<RepairSplit> {
    @Override
    public int partition(RepairSplit repairSplit, int numPartitions) {
        return repairSplit
                .getChannel();
    }
}
