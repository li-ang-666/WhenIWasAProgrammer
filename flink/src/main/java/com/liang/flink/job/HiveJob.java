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

/**
 * drop table if exists flink.query_log;
 * create external table if not exists flink.query_log (
 * id bigint,
 * name string
 * )partitioned by(pt string)
 * ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
 * STORED AS TEXTFILE
 * LOCATION 'obs://hadoop-obs/hive/warehouse/flink.db/query_log/';
 */
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
        private static final DateTimeFormatter PT_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
        private static final String DIR = "obs://hadoop-obs/hive/warehouse/flink.db/query_log/pt=%s/";
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
                String pt = LocalDateTime.now().format(PT_FORMATTER);
                Map<String, Object> columnMap = new HashMap<String, Object>() {{
                    put("id", System.currentTimeMillis() / 1000);
                    put("name", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                }};
                ptToObsWriter
                        .compute(pt, (k, v) -> {
                            ObsWriter obsWriter = (v != null) ? v : new ObsWriter(String.format(DIR, k), ObsWriter.FileFormat.TXT);
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
                String yesterdayPt = LocalDateTime.now().plusDays(-1).format(PT_FORMATTER);
                ptToObsWriter.keySet().removeIf(e -> e.compareTo(yesterdayPt) < 0);
            }
        }
    }

    //@Test
    //public void init() throws Exception {
    //    Class.forName("org.apache.hive.jdbc.HiveDriver");
    //    String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    //    String USER = "hive";
    //    String PASSWORD = "";
    //    Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
    //    LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 1, 0, 0);
    //    int i = 0;
    //    while (true) {
    //        String pt = localDateTime.plusDays(i++).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    //        if (pt.startsWith("2030")) break;
    //        connection.prepareStatement("alter table flink.query_log add if not exists partition(pt = '" + pt + "')").executeUpdate();
    //        System.out.println(pt);
    //    }
    //}
}
