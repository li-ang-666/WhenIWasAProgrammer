package com.liang.flink.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaRecord<T> implements Serializable {
    //基本KV
    private String key;
    private T value;
    //其它信息
    private String topic;
    private int partition;
    private long offset;
    private long reachMilliseconds;
}
