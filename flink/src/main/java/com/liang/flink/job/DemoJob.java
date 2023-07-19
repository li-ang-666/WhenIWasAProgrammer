package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.KafkaSourceFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.KafkaRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        env.setParallelism(1);
        Config config = ConfigUtils.getConfig();
        KafkaSource<KafkaRecord<Tuple2<byte[], String>>> kafkaSource = KafkaSourceFactory.create(e -> Tuple2.of(e, new String(e)));
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .addSink(new DemoSink(config));
        env.execute("DemoJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class DemoSink extends RichSinkFunction<KafkaRecord<Tuple2<byte[], String>>> {
        private final Config config;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void invoke(KafkaRecord<Tuple2<byte[], String>> input, Context context) throws Exception {
            Tuple2<byte[], String> value = input.getValue();
            log.info("binlog: {}", value.f0);
        }
    }
}
