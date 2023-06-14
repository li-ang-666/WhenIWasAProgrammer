package com.liang.flink.service;

import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.common.util.JsonUtils;
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
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaLagReporter implements Runnable {
    private static final int INTERVAL_SECONDS = 180;

    private final Map<TopicPartition, Long> offsetMap;
    private final Map<TopicPartition, Long> timeMap;
    private final AtomicBoolean running;
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;

    public KafkaLagReporter(Map<TopicPartition, Long> offsetMap, Map<TopicPartition, Long> timeMap, AtomicBoolean running) {
        this.offsetMap = offsetMap;
        this.timeMap = timeMap;
        this.running = running;
        kafkaConsumer = new KafkaConsumer<>(new Properties() {{
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ConfigUtils.getConfig().getKafkaConfigs().get("kafkaSource").getBootstrapServers());
        }}, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public void run() {
        while (running.get()) {
            print();
            sleep();
        }
    }

    private void print() {
        Comparator<TopicPartition> mapKeyComparator = (e1, e2) -> e1.topic().equals(e2.topic()) ? e1.partition() - e2.partition() : e1.topic().compareTo(e2.topic());
        if (offsetMap.isEmpty() || timeMap.isEmpty()) {
            log.warn("本轮周期内 kafka 无数据流入");
            return;
        }
        Map<TopicPartition, Long> copyOffsetMap;
        synchronized (offsetMap) {
            copyOffsetMap = new TreeMap<>(mapKeyComparator);
            copyOffsetMap.putAll(offsetMap);
            offsetMap.clear();
        }
        Map<TopicPartition, Long> maxOffsetMap = kafkaConsumer.endOffsets(copyOffsetMap.keySet());
        for (Map.Entry<TopicPartition, Long> entry : maxOffsetMap.entrySet()) {
            TopicPartition key = entry.getKey();
            copyOffsetMap.put(key, entry.getValue() - copyOffsetMap.get(key));
        }
        log.warn("offset lag: {}", JsonUtils.toString(copyOffsetMap));
        Map<TopicPartition, Object> copyTimeMap;
        synchronized (timeMap) {
            copyTimeMap = new TreeMap<>(mapKeyComparator);
            copyTimeMap.putAll(timeMap);
            timeMap.clear();
        }
        for (Map.Entry<TopicPartition, Object> entry : copyTimeMap.entrySet()) {
            TopicPartition key = entry.getKey();
            copyTimeMap.put(key, DateTimeUtils.fromUnixTime((long) entry.getValue() / 1000, "yyyy-MM-dd HH:mm:ss"));
        }
        log.warn("msg time info: {}", JsonUtils.toString(copyTimeMap));
    }

    private void sleep() {
        try {
            TimeUnit.SECONDS.sleep(INTERVAL_SECONDS);
        } catch (Exception ignore) {
        }
    }
}
