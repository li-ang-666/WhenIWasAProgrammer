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
    private static final int INTERVAL_SECONDS = 5;
    private final RedisTemplate redisTemplate = new RedisTemplate("metadata");
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final String kafkaOffsetKey;
    private final String kafkaTimeKey;

    {
        kafkaConsumer = new KafkaConsumer<>(new Properties() {{
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ConfigUtils.getConfig().getKafkaConfigs().get("kafkaSource").getBootstrapServers());
        }}, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    public KafkaLagReporter(String kafkaOffsetKey, String kafkaTimeKey) {
        this.kafkaOffsetKey = kafkaOffsetKey;
        this.kafkaTimeKey = kafkaTimeKey;
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    @SneakyThrows(InterruptedException.class)
    public void run() {
        while (true) {
            Comparator<TopicPartition> mapKeyComparator = (e1, e2) -> e1.topic().equals(e2.topic()) ? e1.partition() - e2.partition() : e1.topic().compareTo(e2.topic());
            Map<String, String> offsetMap = redisTemplate.hScan(kafkaOffsetKey);
            Map<TopicPartition, Long> copyOffsetMap = new TreeMap<>(mapKeyComparator);
            for (Map.Entry<String, String> entry : offsetMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                String[] split = key.split(", ");
                copyOffsetMap.put(new TopicPartition(split[0], Integer.parseInt(split[1])), Long.parseLong(value));
            }
            Map<TopicPartition, Long> maxOffsetMap = kafkaConsumer.endOffsets(copyOffsetMap.keySet());
            int i = 0;
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
                return;
            }
            log.warn("offset lag: {}", JsonUtils.toString(copyOffsetMap));
            Map<String, String> timeMap = redisTemplate.hScan(kafkaTimeKey);
            Map<TopicPartition, String> copyTimeMap = new TreeMap<>(mapKeyComparator);
            for (Map.Entry<String, String> entry : timeMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                String[] split = key.split(", ");
                copyTimeMap.put(new TopicPartition(split[0], Integer.parseInt(split[1])), value);
            }
            for (Map.Entry<TopicPartition, String> entry : copyTimeMap.entrySet()) {
                TopicPartition key = entry.getKey();
                copyTimeMap.put(key, DateTimeUtils.fromUnixTime(Long.parseLong(entry.getValue()), "yyyy-MM-dd HH:mm:ss"));
            }
            log.warn("msg time info: {}", JsonUtils.toString(copyTimeMap));
            TimeUnit.SECONDS.sleep(INTERVAL_SECONDS);
        }
    }
}
