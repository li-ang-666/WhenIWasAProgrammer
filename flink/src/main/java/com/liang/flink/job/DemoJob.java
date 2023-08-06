package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.KafkaSourceFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.KafkaRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Slf4j
@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        KafkaSource<KafkaRecord<String>> kafkaRecordKafkaSource = KafkaSourceFactory.create(bytes -> new String(bytes));
        env.fromSource(kafkaRecordKafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .setParallelism(20)
                .rebalance()
                .addSink(new SinkFunction<KafkaRecord<String>>() {
                    @Override
                    public void invoke(KafkaRecord<String> value, Context context) throws Exception {
                        if (value.getValue().contains("923037552")) {
                            log.error("------------------{}", value);
                        }
                    }
                }).setParallelism(20);
        env.execute("DemoJob");
    }
}
