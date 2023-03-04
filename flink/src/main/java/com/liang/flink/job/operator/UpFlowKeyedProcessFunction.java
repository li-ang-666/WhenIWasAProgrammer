package com.liang.flink.job.operator;

import com.liang.common.dto.config.ConnectionConfig;
import com.liang.common.util.GlobalUtils;
import com.liang.flink.dto.KafkaMessage;
import com.liang.flink.service.StreamPositionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class UpFlowKeyedProcessFunction extends KeyedProcessFunction<Integer, KafkaMessage<String>, KafkaMessage<String>> {

    private StreamPositionService streamPositionService;
    private OutputTag<KafkaMessage<String>> outputTag;
    private ConnectionConfig config;

    public UpFlowKeyedProcessFunction(ConnectionConfig config, OutputTag<KafkaMessage<String>> outputTag) {
        this.config = config;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) {
        GlobalUtils.init(config, null);
        streamPositionService = new StreamPositionService();
    }

    @Override
    public void processElement(KafkaMessage<String> stringKafkaMessage, Context context, Collector<KafkaMessage<String>> collector) {
        String message = stringKafkaMessage.getMessage();
        if (!message.matches(".*?,\\d+")) return;

        String[] orgIdAndUpdateTime = message.split(",");
        String orgId = orgIdAndUpdateTime[0];
        long updateTime = Long.parseLong(orgIdAndUpdateTime[1]);

        int partition = streamPositionService.returnAndCheckPartition(orgId);
        if (partition != -1) {
            // 已经下沉
            stringKafkaMessage.setTopic("moka_hcm_down");
            stringKafkaMessage.setPartition(partition);
            context.output(outputTag, stringKafkaMessage);
        } else if (!streamPositionService.isDownStream(orgId, updateTime)) {
            // !已经下沉 && !需要在本次下沉
            collector.collect(stringKafkaMessage);
        } else {
            int partition_ = streamPositionService.registerPartition(orgId);
            if (partition_ != -1) {
                // !已经下沉 && 需要在本次下沉 && 有可用分区
                stringKafkaMessage.setTopic("moka_hcm_down");
                stringKafkaMessage.setPartition(partition_);
                context.output(outputTag, stringKafkaMessage);
            } else {
                // !已经下沉 && 需要在本次下沉 && !有可用分区
                collector.collect(stringKafkaMessage);
            }
        }
    }
}
