package com.liang.flink.high_level;

import com.liang.flink.basic.FlinkRepairSourceFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class FlinkRepairSourceStreamFactory {
    private FlinkRepairSourceStreamFactory() {
    }

    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment) {
        List<FlinkRepairSourceFactory.FlinkRepairSource> flinkRepairSources = FlinkRepairSourceFactory.create();
        DataStream<SingleCanalBinlog> unionedStream = null;
        for (FlinkRepairSourceFactory.FlinkRepairSource flinkRepairSource : flinkRepairSources) {
            String name = flinkRepairSource.getTask().getName();
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
