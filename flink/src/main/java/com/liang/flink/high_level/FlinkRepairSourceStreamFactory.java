package com.liang.flink.high_level;

import com.liang.common.dto.SubRepairTask;
import com.liang.flink.basic.FlinkRepairSource;
import com.liang.flink.basic.FlinkRepairSourceFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class FlinkRepairSourceStreamFactory {
    private FlinkRepairSourceStreamFactory() {
    }

    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment) {
        List<FlinkRepairSource> flinkFlinkRepairSources = FlinkRepairSourceFactory.create();
        DataStream<SingleCanalBinlog> unionedStream = null;
        for (FlinkRepairSource flinkRepairSource : flinkFlinkRepairSources) {
            SubRepairTask task = flinkRepairSource.getTask();
            String name = String.format("table: %s, uid: %s", task.getTableName(), task.getCheckpointUid());
            DataStream<SingleCanalBinlog> singleStream = streamEnvironment.addSource(flinkRepairSource)
                    .uid(name)
                    .name(name);
            unionedStream = (unionedStream == null) ?
                    singleStream :
                    unionedStream.union(singleStream);
        }
        return unionedStream;
    }
}
