package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.cdc.CanalDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Rds491CdcJob {
    private static final String CDC_HOSTNAME = "505982938db54e86bfc4bd36d49f840din01.internal.cn-north-4.mysql.rds.myhuaweicloud.com";
    private static final String CDC_DATABASE = "prism_shareholder_path";
    private static final String CDC_TABLE = ".*";
    private static final Integer CDC_PORT = 3306;
    private static final String CDC_USERNAME = "canal_d";
    private static final String CDC_PASSWORD = "Canal@Dduan";
    private static final String CDC_SERVER_ID = "491";
    private static final String CDC_TIMEZONE = "Asia/Shanghai";
    private static final StartupOptions CDC_STARTUP_OPTIONS = StartupOptions.latest();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        MySqlSource<FlatMessage> mySqlSource = MySqlSource.<FlatMessage>builder()
                .hostname(CDC_HOSTNAME)
                .port(CDC_PORT)
                .username(CDC_USERNAME)
                .password(CDC_PASSWORD)
                .databaseList(CDC_DATABASE)
                .tableList(CDC_DATABASE + "." + CDC_TABLE)
                .serverId(CDC_SERVER_ID)
                .serverTimeZone(CDC_TIMEZONE)
                .startupOptions(CDC_STARTUP_OPTIONS)
                .deserializer(new CanalDebeziumDeserializationSchema())
                .build();
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "CdcSource")
                .name("CdcSource")
                .uid("CdcSource")
                .setParallelism(1)
                .keyBy(e -> e.getData().get(0).get("company_id"))
                .addSink(new KafkaSink())
                .name("KafkaSink")
                .uid("KafkaSink");
        env.execute("Rds491CdcJob");
    }

    private static final class KafkaSink extends RichSinkFunction<FlatMessage> implements CheckpointedFunction {
        private static final String KAFKA_BOOTSTRAP_SERVER = "10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092";
        private static final String KAFKA_TOPIC = "50598.cdc.prism_shareholder_path.ratio_path_company_new_all";

        private final Lock lock = new ReentrantLock(true);

        private KafkaProducer<byte[], byte[]> producer;
        private int partitionNum;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            // ack
            properties.put(ProducerConfig.ACKS_CONFIG, String.valueOf(1));
            // retry
            properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(60 * 1000));
            properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(3));
            properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(2 * 1000));
            // in order
            properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(1));
            // performance cache time
            properties.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(2 * 1000));
            // performance cache memory
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(64 * 1024 * 1024));
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(64 * 1024 * 1024));
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(2 * 1024 * 1024));
            producer = new KafkaProducer<>(properties);
            partitionNum = producer.partitionsFor(KAFKA_TOPIC).size();
            log.info("partitionNum:{}", partitionNum);
        }

        @Override
        public void invoke(FlatMessage flatMessage, Context context) {
            long companyId = Long.parseLong(flatMessage.getData().get(0).get("company_id"));
            int partition = (int) (companyId % partitionNum);
            byte[] value = JsonUtils.toString(flatMessage).getBytes(StandardCharsets.UTF_8);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(KAFKA_TOPIC, partition, null, value);
            lock.lock();
            producer.send(record);
            lock.unlock();
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
            producer.close();
        }

        private void flush() {
            lock.lock();
            producer.flush();
            lock.unlock();
        }
    }
}
