package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.kafka.KafkaSourceFactory;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@LocalConfigFile("hive.yml")
public class HiveJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        KafkaSource<KafkaRecord<String>> kafkaSource = KafkaSourceFactory.create(String::new);
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .name("KafkaSource")
                .uid("KafkaSource")
                .setParallelism(1)
                .addSink(new HiveSink(config))
                .name("HiveSink")
                .uid("HiveSink")
                .setParallelism(1);
        env.execute("HiveJob");
    }


    @RequiredArgsConstructor
    private static final class HiveSink extends RichSinkFunction<KafkaRecord<String>> implements CheckpointedFunction {
        private static final String DIR = "obs://hadoop-obs/hive/warehouse/flink.db/query_log/";
        private final Map<String, ObsWriter> ptToObsWriter = new HashMap<>();
        private final Config config;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void invoke(KafkaRecord<String> kafkaRecord, Context context) {
            synchronized (ptToObsWriter) {
                String pt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm"));
                Map<String, Object> columnMap = new HashMap<String, Object>() {{
                    put("id", 1);
                    put("name", "lucia");
                }};
                ptToObsWriter.compute(pt, (k, v) -> {
                            ObsWriter obsWriter = (v != null) ? v : new ObsWriter(DIR + pt, ObsWriter.FileFormat.TXT);
                            obsWriter.enableCache();
                            return obsWriter;
                        })
                        .update(JsonUtils.toString(columnMap));
            }
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

        public void flush() {
            synchronized (ptToObsWriter) {
                ptToObsWriter.forEach((pt, ObsWriter) -> ObsWriter.flush());
            }
        }
    }
}
