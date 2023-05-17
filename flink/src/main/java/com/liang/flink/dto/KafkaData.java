package com.liang.flink.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class KafkaData {
    private String key;
    private String value;

    private String topic;
    private long partition;
    private long offset;
    private long timestamp;
}
