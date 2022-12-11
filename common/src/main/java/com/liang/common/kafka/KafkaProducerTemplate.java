package com.liang.common.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerTemplate {
    private KafkaProducer<String, String> producer;

    public KafkaProducerTemplate(String brokerList) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
    }

    public void send(String topic, int partition, String msg) {
        producer.send(new ProducerRecord<>(
                topic, partition, "key", msg
        ));
    }
}
