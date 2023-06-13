package com.liang.flink.high.level.api;

import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.CanalKafkaMonitor;
import com.liang.flink.basic.KafkaSourceFactory;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaStreamFactory {
    private KafkaStreamFactory() {
    }

    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment) {
        KafkaSource<KafkaRecord<BatchCanalBinlog>> kafkaSource = KafkaSourceFactory.create(BatchCanalBinlog::new);
        String name = "KafkaSource";
        return streamEnvironment
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), name)
                .setParallelism(ConfigUtils.getConfig().getFlinkConfig().getSourceParallel())
                .flatMap(new CanalKafkaMonitor(ConfigUtils.getConfig())).name("CanalKafkaMonitor")
                .setParallelism(1);
    }
}
