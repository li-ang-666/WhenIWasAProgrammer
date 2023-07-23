package com.liang.flink.service;

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
    private static final int INTERVAL_SECONDS = 180;

    private final Map<TopicPartition, Long> offsetMap;
    private final Map<TopicPartition, Long> timeMap;
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;

    public KafkaLagReporter(Map<TopicPartition, Long> offsetMap, Map<TopicPartition, Long> timeMap) {
        this.offsetMap = offsetMap;
        this.timeMap = timeMap;
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
            print();
            TimeUnit.SECONDS.sleep(INTERVAL_SECONDS);
        }
    }

    private void print() {
        Comparator<TopicPartition> mapKeyComparator = (e1, e2) -> e1.topic().equals(e2.topic()) ? e1.partition() - e2.partition() : e1.topic().compareTo(e2.topic());
        if (offsetMap.isEmpty() || timeMap.isEmpty()) {
            log.warn("本轮周期内 kafka 无数据流入");
            return;
        }
        Map<TopicPartition, Long> copyOffsetMap = new TreeMap<>(mapKeyComparator);
        synchronized (offsetMap) {
            copyOffsetMap.putAll(offsetMap);
            offsetMap.clear();
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
        Map<TopicPartition, Object> copyTimeMap = new TreeMap<>(mapKeyComparator);
        synchronized (timeMap) {
            copyTimeMap.putAll(timeMap);
            timeMap.clear();
        }
        for (Map.Entry<TopicPartition, Object> entry : copyTimeMap.entrySet()) {
            TopicPartition key = entry.getKey();
            copyTimeMap.put(key, DateTimeUtils.fromUnixTime((long) entry.getValue(), "yyyy-MM-dd HH:mm:ss"));
        }
        log.warn("msg time info: {}", JsonUtils.toString(copyTimeMap));
    }
}
