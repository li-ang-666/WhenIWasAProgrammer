package com.liang.flink.service;

import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaLagReporter implements Runnable {
    private final Map<TopicPartition, Long> offsetMap;
    private final Map<TopicPartition, Long> timeMap;
    private final AtomicBoolean running;
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;

    public KafkaLagReporter(Map<TopicPartition, Long> offsetMap, Map<TopicPartition, Long> timeMap, AtomicBoolean running) {
        this.offsetMap = offsetMap;
        this.timeMap = timeMap;
        this.running = running;

        Properties properties = new Properties() {{
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ConfigUtils.getConfig().getKafkaConfigs().get("kafkaSource").getBootstrapServers());
        }};
        kafkaConsumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public void run() {
        while (running.get()) {
            if (!offsetMap.isEmpty()) {
                Map<TopicPartition, Long> maxOffsetMap = kafkaConsumer.endOffsets(offsetMap.keySet());
                HashMap<TopicPartition, Long> printMap = new HashMap<>();
                synchronized (offsetMap) {
                    for (Map.Entry<TopicPartition, Long> entry : maxOffsetMap.entrySet()) {
                        TopicPartition key = entry.getKey();
                        printMap.put(key, entry.getValue() - offsetMap.get(key));
                    }
                    log.warn("offset lag: {}", JsonUtils.toString(printMap));
                    offsetMap.clear();
                }
            }
            if (!timeMap.isEmpty()) {
                HashMap<TopicPartition, Long> printMap = new HashMap<>();
                synchronized (timeMap) {
                    for (Map.Entry<TopicPartition, Long> entry : timeMap.entrySet()) {
                        TopicPartition key = entry.getKey();
                        printMap.put(key, (System.currentTimeMillis() - timeMap.get(key)) / 1000);
                    }
                    log.warn("time lag: {}", JsonUtils.toString(printMap));
                    timeMap.clear();
                }
            }
            try {
                TimeUnit.SECONDS.sleep(60);
            } catch (Exception ignore) {
            }
        }
    }
}
