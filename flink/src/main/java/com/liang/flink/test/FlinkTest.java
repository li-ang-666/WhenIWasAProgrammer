package com.liang.flink.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FlinkTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partition = new TopicPartition("140f5.proto.bigdata_online.itch_point_secondary_dims_uv_count", 0);
        try {
            Map<TopicPartition, Long> topicPartition2OffsetMap = consumer.endOffsets(Collections.singletonList(partition), Duration.ofMillis(100));
        } catch (Exception e) {

        }

        System.out.println("aaaaaaaa");
        //System.out.println(topicPartition2OffsetMap);
        //System.out.println(topicPartition2OffsetMap.get(partition));
    }
}
