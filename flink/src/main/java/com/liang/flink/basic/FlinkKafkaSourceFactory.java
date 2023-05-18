package com.liang.flink.basic;

import com.liang.common.dto.config.KafkaConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.function.KafkaRecordValueMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

@Slf4j
public class FlinkKafkaSourceFactory {
    private FlinkKafkaSourceFactory() {
    }

    public static <T> FlinkKafkaConsumer<KafkaRecord<T>> createFlinkKafkaStream(KafkaRecordValueMapper<T> mapper) {
        SimpleKafkaDeserializationSchema<T> schema = new SimpleKafkaDeserializationSchema<>(mapper);
        KafkaConfig kafkaConfig = ConfigUtils.getConfig().getKafkaConfigs().get("consumer00");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaConfig.getBootstrapServers());
        properties.setProperty("group.id", kafkaConfig.getGroupId());
        FlinkKafkaConsumer<KafkaRecord<T>> flinkKafkaConsumer = new FlinkKafkaConsumer<>(kafkaConfig.getTopics(), schema, properties);
        //checkpoint时提交offset
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        //如果没有checkpoint,从哪里消费
        switch (kafkaConfig.getStartFrom()) {
            case "0":
                flinkKafkaConsumer.setStartFromGroupOffsets();
                break;
            case "1":
                flinkKafkaConsumer.setStartFromEarliest();
                break;
            case "2":
                flinkKafkaConsumer.setStartFromLatest();
                break;
            default:
                //毫秒时间戳
                flinkKafkaConsumer.setStartFromTimestamp(Long.parseLong(kafkaConfig.getStartFrom()));
        }
        return flinkKafkaConsumer;
    }

    private static class SimpleKafkaDeserializationSchema<T> implements KafkaDeserializationSchema<KafkaRecord<T>> {

        private final KafkaRecordValueMapper<T> mapper;

        public SimpleKafkaDeserializationSchema(KafkaRecordValueMapper<T> mapper) {
            this.mapper = mapper;
        }

        @Override
        public boolean isEndOfStream(KafkaRecord<T> nextElement) {
            return false;
        }

        @Override
        public KafkaRecord<T> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            byte[] keyBytes = record.key();
            byte[] valueBytes = record.value();
            String key = keyBytes != null ? new String(keyBytes) : null;
            T value = mapper.map(valueBytes);
            String topic = record.topic();
            int partition = record.partition();
            long offset = record.offset();
            long timestamp = record.timestamp();
            return new KafkaRecord<T>(key, value, topic, partition, offset, timestamp);
        }

        @Override
        public TypeInformation<KafkaRecord<T>> getProducedType() {
            return null;
        }
    }
}
