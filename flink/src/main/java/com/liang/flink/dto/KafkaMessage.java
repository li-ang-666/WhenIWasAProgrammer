package com.liang.flink.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class KafkaMessage<T> implements Serializable {
    private String topic;
    private int partition;
    private long offset;
    private String createTime;
    private T message;
}
