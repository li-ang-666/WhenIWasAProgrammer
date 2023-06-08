package com.liang.flink.high.level.api;

import com.liang.common.dto.SubRepairTask;
import com.liang.flink.basic.RepairSource;
import com.liang.flink.basic.RepairSourceFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class RepairSourceStreamFactory {
    private RepairSourceStreamFactory() {
    }

    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment) {
        List<RepairSource> flinkRepairSources = RepairSourceFactory.create();
        DataStream<SingleCanalBinlog> unionedStream = null;
        for (RepairSource repairSource : flinkRepairSources) {
            SubRepairTask task = repairSource.getTask();
            String name = String.format("RepairSource(table=%s, uid=%s)", task.getTableName(), task.getCheckpointUid());
            DataStream<SingleCanalBinlog> singleStream = streamEnvironment.addSource(repairSource)
                    .uid(name)
                    .name(name);
            unionedStream = (unionedStream == null) ?
                    singleStream :
                    unionedStream.union(singleStream);
        }
        return unionedStream;
    }
}
