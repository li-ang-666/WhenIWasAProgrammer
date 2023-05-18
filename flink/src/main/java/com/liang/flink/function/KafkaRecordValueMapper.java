package com.liang.flink.function;


import java.io.Serializable;

@FunctionalInterface
public interface KafkaRecordValueMapper<T> extends Serializable {
    T map(byte[] kafkaRecord) throws Exception;
}
