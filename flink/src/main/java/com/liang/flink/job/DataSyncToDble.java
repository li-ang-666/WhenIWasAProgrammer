package com.liang.flink.job;

import com.liang.common.dto.config.ConnectionConfig;
import com.liang.common.util.YamlUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.common.util.FlinkKeyByUtils;
import com.liang.flink.dto.KafkaMessage;
import com.liang.flink.env.StreamEnvironmentFactory;
import com.liang.flink.job.operator.DownFlowKeyedProcessFunction;
import com.liang.flink.job.operator.UpFlowKeyedProcessFunction;
import com.liang.flink.job.sink.KafkaSink;
import com.liang.flink.job.source.ConsumerRecordMapper;
import com.liang.flink.job.source.KafkaSourceFactory;
import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;

import static com.liang.common.util.SimpleConstructUtils.newList;

@val
public class DataSyncToDble {
    public static void main(String[] args) throws Exception {
        val env = StreamEnvironmentFactory.createStreamEnvironment(args);
        ConsumerRecordMapper<KafkaMessage<String>> consumerRecordMapper = consumerRecord -> {
            KafkaMessage<String> stringKafkaMessage = new KafkaMessage<>();
            stringKafkaMessage.setTopic(consumerRecord.topic());
            stringKafkaMessage.setPartition(consumerRecord.partition());
            stringKafkaMessage.setOffset(consumerRecord.offset());
            stringKafkaMessage.setCreateTime(DateTimeUtils.fromUnixTime(consumerRecord.timestamp() / 1000, "yyyy-MM-dd HH:mm:ss"));
            stringKafkaMessage.setMessage(new String(consumerRecord.value(), StandardCharsets.UTF_8));
            return stringKafkaMessage;
        };
        val typeInformation = TypeInformation.of(
                new TypeHint<KafkaMessage<String>>() {
                });
        val heavyKeyMapper = FlinkKeyByUtils.getBalanceMapper(
                newList("xiaomi", "qunar", "sohu"), env.getParallelism()
        );
        val heavyKeyMapper2 = FlinkKeyByUtils.getBalanceMapper(
                newList(0, 1, 2, 3, 4, 5), env.getParallelism()
        );
        val outputTag = new OutputTag<KafkaMessage<String>>("side-output") {
        };
        val config = YamlUtils.fromResource("connections.yml", ConnectionConfig.class);
        /*-----------------------------------------------------------------------------------*/
        val kafkaStream = env.addSource(
                KafkaSourceFactory.createKafkaSource(
                        "172.17.89.173:9092,172.17.89.174:9092,172.17.89.175:9092", "bi_test_staging002_down", "_test_", consumerRecordMapper, typeInformation));
        val keyedStream = kafkaStream.keyBy(
                (KeySelector<KafkaMessage<String>, Integer>) value -> {
                    String[] OrgIdAndTime = value.getMessage().split(",");
                    return heavyKeyMapper.getOrDefault(OrgIdAndTime[0], OrgIdAndTime[0].hashCode());
                });
        val processedUpStream = keyedStream.process(
                new UpFlowKeyedProcessFunction(config, outputTag));
        val processedDownStream = processedUpStream.getSideOutput(
                outputTag);

        processedUpStream.print(
                "up flow");
        processedDownStream.addSink(
                new KafkaSink(config));
        /*-----------------------------------------------------------------------------------*/
        val kafkaStream2 = env.addSource(
                KafkaSourceFactory.createKafkaSource("172.17.89.173:9092,172.17.89.174:9092,172.17.89.175:9092", "moka_hcm_down", "liang_test", consumerRecordMapper, typeInformation));
        val keyedStream2 = kafkaStream2.keyBy(
                (KeySelector<KafkaMessage<String>, Integer>) value -> heavyKeyMapper2.getOrDefault(value.getPartition(), Integer.hashCode(value.getPartition())));
        val processedStream2 = keyedStream2.process(
                new DownFlowKeyedProcessFunction(config));
        processedStream2.print(
                "down flow");

        env.execute("LI_ANG_JOB_TO_DBLE");
    }
}
