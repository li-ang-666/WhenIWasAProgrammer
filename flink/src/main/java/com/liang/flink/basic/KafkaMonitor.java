package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.KafkaLagReporter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KafkaMonitor extends RichFlatMapFunction<KafkaRecord<BatchCanalBinlog>, SingleCanalBinlog> {
    private final Config config;
    private final Map<TopicPartition, Long> offsetMap = new HashMap<>();
    private final Map<TopicPartition, Long> timeMap = new HashMap<>();

    public KafkaMonitor(Config config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        DaemonExecutor.launch("KafkaLagReporter", new KafkaLagReporter(offsetMap, timeMap));
    }

    @Override
    public void flatMap(KafkaRecord<BatchCanalBinlog> kafkaRecord, Collector<SingleCanalBinlog> out) {
        TopicPartition key = new TopicPartition(kafkaRecord.getTopic(), kafkaRecord.getPartition());
        synchronized (offsetMap) {
            offsetMap.put(key, kafkaRecord.getOffset());
        }
        synchronized (timeMap) {
            timeMap.put(key, kafkaRecord.getReachMilliseconds() / 1000);
        }
        for (SingleCanalBinlog singleCanalBinlog : kafkaRecord.getValue().getSingleCanalBinlogs()) {
            out.collect(singleCanalBinlog);
        }
    }
}

