package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.KafkaConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.flink.dto.KafkaRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.connector.kafka.source.KafkaSourceOptions.*;

@Slf4j
public class KafkaSourceFactory {
    private KafkaSourceFactory() {
    }

    public static <T> KafkaSource<KafkaRecord<T>> create(KafkaRecordValueMapper<T> mapper) {
        Config config = ConfigUtils.getConfig();
        KafkaConfig kafkaConfig = config.getKafkaConfigs().get("kafkaSource");
        //如果没有checkpoint,从哪里消费
        String startFrom = kafkaConfig.getStartFrom();
        OffsetsInitializer offsetsInitializer;
        switch (startFrom) {
            case "0":
                offsetsInitializer = OffsetsInitializer.committedOffsets();
                break;
            case "1":
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case "2":
                offsetsInitializer = OffsetsInitializer.latest();
                break;
            default:
                long timestamp = DateTimeUtils.unixTimestamp(startFrom, "yyyy-MM-dd HH:mm:ss");
                offsetsInitializer = OffsetsInitializer.timestamp(timestamp * 1000L);
                break;
        }
        return KafkaSource.<KafkaRecord<T>>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setGroupId(kafkaConfig.getGroupId())
                .setTopics(kafkaConfig.getTopics())
                .setDeserializer(new KafkaDeserializationSchema<>(config, mapper))
                .setProperty(PARTITION_DISCOVERY_INTERVAL_MS.key(), "60000")
                .setProperty(REGISTER_KAFKA_CONSUMER_METRICS.key(), "false")
                .setProperty(COMMIT_OFFSETS_ON_CHECKPOINT.key(), "true")
                .setStartingOffsets(offsetsInitializer)
                .build();
    }


    @FunctionalInterface
    public interface KafkaRecordValueMapper<T> extends Serializable {
        T map(byte[] kafkaRecord);
    }

    private static class KafkaDeserializationSchema<T> implements KafkaRecordDeserializationSchema<KafkaRecord<T>> {
        private final Config config;
        private final KafkaRecordValueMapper<T> mapper;

        public KafkaDeserializationSchema(Config config, KafkaRecordValueMapper<T> mapper) {
            this.config = config;
            this.mapper = mapper;
        }

        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaRecord<T>> out) throws IOException {
            //基本KV
            byte[] keyBytes = record.key();
            byte[] valueBytes = record.value();
            String key = keyBytes != null ? new String(keyBytes) : null;
            T value = mapper.map(valueBytes);
            //其它信息
            String topic = record.topic();
            int partition = record.partition();
            long offset = record.offset();
            long timestamp = record.timestamp();
            out.collect(new KafkaRecord<>(key, value, topic, partition, offset, timestamp));
        }

        @Override
        public TypeInformation<KafkaRecord<T>> getProducedType() {
            return TypeInformation.of(new TypeHint<KafkaRecord<T>>() {
            });
        }
    }
}
