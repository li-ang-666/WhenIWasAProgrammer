package com.liang.flink.high.level.api;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.KafkaMonitor;
import com.liang.flink.basic.KafkaSourceFactory;
import com.liang.flink.basic.RepairSource;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.liang.common.dto.config.FlinkConfig.SourceType.Kafka;

@UtilityClass
@Slf4j
public class StreamFactory {
    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment) {
        Config config = ConfigUtils.getConfig();
        return config.getFlinkConfig().getSourceType() == Kafka ?
                createKafkaStream(streamEnvironment) :
                createRepairStream(streamEnvironment);
    }

    private static DataStream<SingleCanalBinlog> createKafkaStream(StreamExecutionEnvironment streamEnvironment) {
        KafkaSource<KafkaRecord<BatchCanalBinlog>> kafkaSource = KafkaSourceFactory.create(BatchCanalBinlog::new);
        return streamEnvironment
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .setParallelism(ConfigUtils.getConfig().getFlinkConfig().getSourceParallel())
                .flatMap(new KafkaMonitor(ConfigUtils.getConfig())).name("Monitor")
                .setParallelism(1);
    }

    private static DataStream<SingleCanalBinlog> createRepairStream(StreamExecutionEnvironment streamEnvironment) {
        Config config = ConfigUtils.getConfig();
        int size = config.getRepairTasks().size();
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        StackTraceElement traceElement = stackTrace[stackTrace.length - 1];
        String JobClassName = traceElement.getClassName();
        new RedisTemplate("metadata").del(JobClassName);
        return streamEnvironment
                .addSource(new RepairSource(config, JobClassName))
                .name("RepairSource")
                .setParallelism(size);
    }
}
