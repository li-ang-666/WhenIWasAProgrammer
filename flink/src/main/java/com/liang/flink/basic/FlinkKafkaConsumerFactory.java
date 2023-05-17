package com.liang.flink.basic;

import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.protocol.Message;
import com.liang.common.dto.config.KafkaConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.KafkaData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class FlinkKafkaConsumerFactory {
    private FlinkKafkaConsumerFactory() {
    }

    public static FlinkKafkaConsumer<KafkaData> createFlinkKafkaStream() {
        SimpleKafkaDeserializationSchema schema = SimpleKafkaDeserializationSchema.getInstance();
        KafkaConfig kafkaConfig = ConfigUtils.getConfig().getKafkaConfigs().get("consumer00");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaConfig.getBootstrapServers());
        properties.setProperty("group.id", kafkaConfig.getGroupId());
        FlinkKafkaConsumer<KafkaData> flinkKafkaConsumer = new FlinkKafkaConsumer<>(kafkaConfig.getTopics(), schema, properties);
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

    private static class SimpleKafkaDeserializationSchema implements KafkaDeserializationSchema<KafkaData> {
        private static volatile SimpleKafkaDeserializationSchema instance;

        private static SimpleKafkaDeserializationSchema getInstance() {
            if (instance == null) {
                synchronized (SimpleKafkaDeserializationSchema.class) {
                    if (instance == null) {
                        instance = new SimpleKafkaDeserializationSchema();
                    }
                }
            }
            return instance;
        }

        @Override
        public boolean isEndOfStream(KafkaData nextElement) {
            return false;
        }

        @Override
        public KafkaData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            byte[] key = record.key();
            byte[] value = record.value();
            Message message = CanalMessageDeserializer.deserializer(value);
            String topic = record.topic();
            int partition = record.partition();
            long offset = record.offset();
            long timestamp = record.timestamp();
            return new KafkaData(key != null ? new String(key) : null, message.toString(), topic, partition, offset, timestamp);
        }

        @Override
        public TypeInformation<KafkaData> getProducedType() {
            return TypeInformation.of(KafkaData.class);
        }
    }
}
