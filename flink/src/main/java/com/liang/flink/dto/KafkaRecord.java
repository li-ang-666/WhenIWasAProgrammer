package com.liang.flink.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class KafkaRecord<T> implements Serializable {
    private String key;
    private T value;

    private String topic;
    private long partition;
    private long offset;
    private long reachMilliseconds;
}
