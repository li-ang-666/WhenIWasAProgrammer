package com.liang.flink.job.source;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

@FunctionalInterface
public interface ConsumerRecordMapper<T> extends Serializable {
    T map(ConsumerRecord<byte[], byte[]> kafkaConsumer) throws Exception;
}
