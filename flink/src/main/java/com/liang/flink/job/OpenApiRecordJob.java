package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/*
 * drop table if exists flink.open_api_record;
 * create external table if not exists flink.open_api_record(
 *   id bigint,
 *   name string,
 *   age int
 * )partitioned by(pt string)
 * ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
 * STORED AS TEXTFILE;
 * MSCK REPAIR TABLE flink.open_api_record;
 */
@LocalConfigFile("open-api-record-job.yml")
public class OpenApiRecordJob {
    // common
    private static final String DATABASE = "flink";
    private static final String TABLE = "open_api_record";
    // hive jdbc
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";
    private static final int PARTITION_FLUSH_INTERVAL_MILLI = 1000 * 60;
    // obs
    private static final DateTimeFormatter PT_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final String DIR = "obs://hadoop-obs/hive/warehouse/" + DATABASE + ".db/" + TABLE + "/pt=%s/";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        KafkaSource<KafkaRecord<String>> kafkaSource = KafkaSourceFactory.create(String::new);
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .name("KafkaSource")
                .uid("KafkaSource")
                .setParallelism(1)
                .addSink(new OpenApiRecordSink(config))
                .name("OpenApiRecordSink")
                .uid("OpenApiRecordSink")
                .setParallelism(1);
        DaemonExecutor.launch("PartitionRepairer", () -> {
            try {
                Class.forName(DRIVER);
                while (true) {
                    try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
                        connection.prepareStatement("MSCK REPAIR TABLE " + DATABASE + "." + TABLE).executeUpdate();
                        LockSupport.parkUntil(System.currentTimeMillis() + PARTITION_FLUSH_INTERVAL_MILLI);
                    } catch (Exception ignore) {
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        env.execute("OpenApiRecordJob");
    }

    @RequiredArgsConstructor
    private static final class OpenApiRecordSink extends RichSinkFunction<KafkaRecord<String>> implements CheckpointedFunction {
        private final Map<String, ObsWriter> pt2ObsWriter = new HashMap<>();
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
            synchronized (pt2ObsWriter) {
                String pt = LocalDateTime.now().format(PT_FORMATTER);
                Map<String, Object> columnMap = new HashMap<String, Object>() {{
                    put("id", System.currentTimeMillis() / 1000);
                    put("name", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    put("age", 80);
                }};
                pt2ObsWriter
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
            synchronized (pt2ObsWriter) {
                pt2ObsWriter.forEach((pt, ObsWriter) -> ObsWriter.flush());
                String yesterdayPt = LocalDateTime.now().plusDays(-1).format(PT_FORMATTER);
                pt2ObsWriter.keySet().removeIf(e -> e.compareTo(yesterdayPt) < 0);
            }
        }
    }
}
