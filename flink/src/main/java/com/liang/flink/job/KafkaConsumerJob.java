package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.kafka.KafkaSourceFactory;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@Slf4j
@LocalConfigFile("kafka-consumer.yml")
public class KafkaConsumerJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        KafkaSource<KafkaRecord<String>> kafkaSource = KafkaSourceFactory.create(String::new);
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .setParallelism(1)
                .map(KafkaRecord::getValue)
                .returns(String.class)
                .setParallelism(1)
                .addSink(new KafkaConsumerSink(config))
                .setParallelism(1);
        env.execute("KafkaConsumerJob");
    }

    @RequiredArgsConstructor
    private final static class KafkaConsumerSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Config config;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void invoke(String value, Context context) {
            if (value.contains("UPDATE"))
                System.out.println(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
        }

        @Override
        public void finish() {
            flush();
        }

        @Override
        public void close() {
            flush();
        }

        private void flush() {
        }
    }
}
