package com.liang.flink.basic.kafka;

import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.common.util.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@RequiredArgsConstructor
public class KafkaLagReporter implements Runnable {
    private static final int READ_REDIS_INTERVAL_MILLISECONDS = 1000 * 60;
    private static final Comparator<TopicPartition> TOPIC_PARTITION_COMPARATOR = (e1, e2) -> e1.topic().equals(e2.topic()) ? e1.partition() - e2.partition() : e1.topic().compareTo(e2.topic());
    private final RedisTemplate redisTemplate = new RedisTemplate("metadata");
    private final String kafkaOffsetKey;
    private final String kafkaTimeKey;

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.getConfig().getKafkaConfigs().get("kafkaSource").getBootstrapServers());
        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        while (true) {
            LockSupport.parkUntil(System.currentTimeMillis() + READ_REDIS_INTERVAL_MILLISECONDS);
            Map<String, String> offsetMap = redisTemplate.hScan(kafkaOffsetKey);
            // 格式化
            Map<TopicPartition, Long> copyOffsetMap = new TreeMap<>(TOPIC_PARTITION_COMPARATOR);
            for (Map.Entry<String, String> entry : offsetMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                String[] split = key.split("@");
                copyOffsetMap.put(new TopicPartition(split[0], Integer.parseInt(split[1])), Long.parseLong(value));
            }
            // 获取最大值
            Map<TopicPartition, Long> maxOffsetMap = kafkaConsumer.endOffsets(copyOffsetMap.keySet());
            int i = 0;
            // 计算
            for (Map.Entry<TopicPartition, Long> entry : maxOffsetMap.entrySet()) {
                TopicPartition key = entry.getKey();
                long lag = entry.getValue() - copyOffsetMap.get(key);
                copyOffsetMap.put(key, lag);
                if (lag > 100L) {
                    i++;
                }
            }
            if (i == 0) {
                log.info("本轮周期内 kafka 所有分区 lag 均小于 100");
                continue;
            }
            log.warn("kafka offset lag: {}", JsonUtils.toString(copyOffsetMap));
            Map<String, String> timeMap = redisTemplate.hScan(kafkaTimeKey);
            // 格式化 and 计算
            Map<TopicPartition, String> copyTimeMap = new TreeMap<>(TOPIC_PARTITION_COMPARATOR);
            for (Map.Entry<String, String> entry : timeMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                String[] split = key.split("@");
                TopicPartition topicPartition = new TopicPartition(split[0], Integer.parseInt(split[1]));
                String time = DateTimeUtils.fromUnixTime(Long.parseLong(value), "yyyy-MM-dd HH:mm:ss");
                copyTimeMap.put(topicPartition, time);
            }
            log.warn("kafka time info: {}", JsonUtils.toString(copyTimeMap));
        }
    }
}
