package com.liang.flink.basic;

import com.liang.flink.dto.KafkaRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;

@Slf4j
public class RichMapMonitor<T> extends RichMapFunction<KafkaRecord<T>, KafkaRecord<T>> {
    @Override
    public KafkaRecord<T> map(KafkaRecord<T> kafkaRecord) throws Exception {
        String topic = kafkaRecord.getTopic();
        long partition = kafkaRecord.getPartition();
        long offset = kafkaRecord.getOffset();
        long maxOffset = kafkaRecord.getTopicPartitionMaxOffset();
        if (maxOffset - offset >= 1000L) {
            log.warn("topic: {}, partition: {}, offset lag: {}", topic, partition, maxOffset - offset);
        }
        long reachMilliseconds = kafkaRecord.getReachMilliseconds();
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - reachMilliseconds >= 1000 * 30L) {
            log.warn("topic: {}, partition: {}, time lag: {}s", topic, partition, (currentTimeMillis - reachMilliseconds) / (float) 1000);
        }
        return kafkaRecord;
    }
}
