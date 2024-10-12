package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
@LocalConfigFile("relation-export.yml")
public class RelationKafkaJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new RelationKafkaSink(config))
                .name("RelationExportSink")
                .uid("RelationExportSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RelationKafkaJob");
    }

    @RequiredArgsConstructor
    private static final class RelationKafkaSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private static final String BOOTSTRAP_SERVER = "10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092";
        private static final String TOPIC = "rds.json.relation.edge";
        private final Config config;
        private final Lock lock = new ReentrantLock(true);
        private KafkaProducer<byte[], byte[]> producer;
        private int partition;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
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
            properties.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(1000));
            // performance cache memory
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(16 * 1024 * 1024));
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(16 * 1024 * 1024));
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(1024 * 1024));
            producer = new KafkaProducer<>(properties);
            partition = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            try {
                lock.lock();
                byte[] value = JsonUtils.toString(flatMessage).getBytes(StandardCharsets.UTF_8);
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, partition, null, value);
                producer.send(record);
            }finally {
                lock.unlock();
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
            producer.close();
        }

        private void flush() {
            lock.lock();
            producer.flush();
            lock.unlock();
        }
    }
}
