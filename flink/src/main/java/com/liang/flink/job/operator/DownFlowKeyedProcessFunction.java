package com.liang.flink.job.operator;

import com.liang.common.dto.config.ConnectionConfig;
import com.liang.common.util.GlobalUtils;
import com.liang.flink.dto.KafkaMessage;
import com.liang.flink.service.StreamPositionService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DownFlowKeyedProcessFunction extends KeyedProcessFunction<Integer, KafkaMessage<String>, KafkaMessage<String>> {
    private StreamPositionService streamPositionService;
    private ConnectionConfig config;

    public DownFlowKeyedProcessFunction(ConnectionConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        GlobalUtils.init(config, null);
        streamPositionService = new StreamPositionService();
    }

    @Override
    public void processElement(KafkaMessage<String> value, Context ctx, Collector<KafkaMessage<String>> out) {
        String orgId = value.getMessage().split(",")[0];
        int partition = value.getPartition();
        streamPositionService.flushPartition(partition, orgId);
        out.collect(value);
    }
}
