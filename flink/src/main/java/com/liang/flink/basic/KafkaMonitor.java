package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.TopicPartition;

@Slf4j
@RequiredArgsConstructor
public class KafkaMonitor extends RichFlatMapFunction<KafkaRecord<BatchCanalBinlog>, SingleCanalBinlog> {
    private final Config config;
    private final String kafkaOffsetKey;
    private final String kafkaTimeKey;
    private RedisTemplate redisTemplate;

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        redisTemplate = new RedisTemplate("metadata");
    }

    @Override
    public void flatMap(KafkaRecord<BatchCanalBinlog> kafkaRecord, Collector<SingleCanalBinlog> out) {
        TopicPartition topicPartition = new TopicPartition(kafkaRecord.getTopic(), kafkaRecord.getPartition());
        String key = topicPartition.topic() + ", " + topicPartition.partition();
        redisTemplate.hSet(kafkaOffsetKey, key, String.valueOf(kafkaRecord.getOffset()));
        redisTemplate.hSet(kafkaTimeKey, key, String.valueOf(kafkaRecord.getReachMilliseconds() / 1000));
        for (SingleCanalBinlog singleCanalBinlog : kafkaRecord.getValue().getSingleCanalBinlogs()) {
            out.collect(singleCanalBinlog);
        }
    }
}

