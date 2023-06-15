package com.liang.flink.high.level.api;

import com.liang.common.dto.Config;
import com.liang.flink.dto.SubRepairTask;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.CanalKafkaMonitor;
import com.liang.flink.basic.KafkaSourceFactory;
import com.liang.flink.basic.RepairSource;
import com.liang.flink.basic.RepairSourceFactory;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

import static com.liang.common.dto.config.FlinkConfig.SourceType.Kafka;

public class CanalBinlogStreamFactory {
    private CanalBinlogStreamFactory() {
    }

    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment) {
        Config config = ConfigUtils.getConfig();
        return config.getFlinkConfig().getSourceType() == Kafka ?
                createKafkaStream(streamEnvironment) :
                createRepairStream(streamEnvironment);
    }

    private static DataStream<SingleCanalBinlog> createKafkaStream(StreamExecutionEnvironment streamEnvironment) {
        KafkaSource<KafkaRecord<BatchCanalBinlog>> kafkaSource = KafkaSourceFactory.create(BatchCanalBinlog::new);
        String name = "KafkaSource";
        return streamEnvironment
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), name)
                .setParallelism(ConfigUtils.getConfig().getFlinkConfig().getSourceParallel())
                .flatMap(new CanalKafkaMonitor(ConfigUtils.getConfig())).name("CanalKafkaMonitor")
                .setParallelism(1);
    }

    private static DataStream<SingleCanalBinlog> createRepairStream(StreamExecutionEnvironment streamEnvironment) {
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
