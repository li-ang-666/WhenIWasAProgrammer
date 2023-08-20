package com.liang.flink.service;

import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.common.util.JsonUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaLagReporter implements Runnable {
    private static final int INTERVAL_SECONDS = 60 * 3;

    private final Comparator<TopicPartition> mapKeyComparator = (e1, e2) -> e1.topic().equals(e2.topic()) ? e1.partition() - e2.partition() : e1.topic().compareTo(e2.topic());
    private final RedisTemplate redisTemplate = new RedisTemplate("metadata");
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final String kafkaOffsetKey;
    private final String kafkaTimeKey;

    public KafkaLagReporter(String kafkaOffsetKey, String kafkaTimeKey) {
        this.kafkaOffsetKey = kafkaOffsetKey;
        this.kafkaTimeKey = kafkaTimeKey;
        kafkaConsumer = new KafkaConsumer<>(new Properties() {{
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ConfigUtils.getConfig().getKafkaConfigs().get("kafkaSource").getBootstrapServers());
        }}, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    @SneakyThrows(InterruptedException.class)
    public void run() {
        while (true) {
            TimeUnit.SECONDS.sleep(INTERVAL_SECONDS);
            Map<String, String> offsetMap = redisTemplate.hScan(kafkaOffsetKey);
            // 格式化
            Map<TopicPartition, Long> copyOffsetMap = new TreeMap<>(mapKeyComparator);
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
            // 格式化
            Map<TopicPartition, String> copyTimeMap = new TreeMap<>(mapKeyComparator);
            for (Map.Entry<String, String> entry : timeMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                String[] split = key.split("@");
                copyTimeMap.put(new TopicPartition(split[0], Integer.parseInt(split[1])), value);
            }
            // 计算
            for (Map.Entry<TopicPartition, String> entry : copyTimeMap.entrySet()) {
                TopicPartition key = entry.getKey();
                copyTimeMap.put(key, DateTimeUtils.fromUnixTime(Long.parseLong(entry.getValue()), "yyyy-MM-dd HH:mm:ss"));
            }
            log.warn("kafka time info: {}", JsonUtils.toString(copyTimeMap));
        }
    }
}
