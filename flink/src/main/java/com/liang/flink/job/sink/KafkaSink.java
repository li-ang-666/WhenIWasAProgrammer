package com.liang.flink.job.sink;

import com.liang.common.kafka.KafkaProducerTemplate;
import com.liang.common.dto.config.ConnectionConfig;
import com.liang.common.util.GlobalUtils;
import com.liang.flink.dto.KafkaMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class KafkaSink extends RichSinkFunction<KafkaMessage<String>> {
    private ConnectionConfig config;
    private KafkaProducerTemplate kafkaProducerTemplate;

    public KafkaSink(ConnectionConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        GlobalUtils.init(config, null);
        kafkaProducerTemplate = new KafkaProducerTemplate("172.17.89.173:9092,172.17.89.174:9092,172.17.89.175:9092");
    }

    @Override
    public void invoke(KafkaMessage<String> value, Context context) {
        kafkaProducerTemplate.send(value.getTopic(), value.getPartition(), value.getMessage());
    }
}
