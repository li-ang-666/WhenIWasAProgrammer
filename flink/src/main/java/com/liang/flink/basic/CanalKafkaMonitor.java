package com.liang.flink.basic;

import com.liang.common.dto.Config;
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
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CanalKafkaMonitor extends RichFlatMapFunction<KafkaRecord<BatchCanalBinlog>, SingleCanalBinlog> {
    private final Config config;
    private final Map<TopicPartition, Long> offsetMap = new HashMap<>();
    private final Map<TopicPartition, Long> timeMap = new HashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public CanalKafkaMonitor(Config config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ConfigUtils.setConfig(config);
        new Thread(new KafkaLagReporter(offsetMap, timeMap, running)).start();
    }

    @Override
    public void flatMap(KafkaRecord<BatchCanalBinlog> kafkaRecord, Collector<SingleCanalBinlog> out) throws Exception {
        TopicPartition key = new TopicPartition(kafkaRecord.getTopic(), kafkaRecord.getPartition());
        synchronized (offsetMap) {
            offsetMap.put(key, kafkaRecord.getOffset());
        }
        synchronized (timeMap) {
            timeMap.put(key, kafkaRecord.getReachMilliseconds());
        }
        for (SingleCanalBinlog singleCanalBinlog : kafkaRecord.getValue().getSingleCanalBinlogs()) {
            out.collect(singleCanalBinlog);
        }
    }

    @Override
    public void close() throws Exception {
        running.set(false);
    }
}

