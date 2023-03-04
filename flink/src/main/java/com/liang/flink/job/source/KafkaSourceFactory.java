package com.liang.flink.job.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaSourceFactory {
    private KafkaSourceFactory() {
    }

    public static <T> FlinkKafkaConsumer<T> createKafkaSource(String brokerList, String topicRegex, String groupId, ConsumerRecordMapper<T> consumerRecordMapper, TypeInformation<T> typeInformation) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        FlinkKafkaConsumer<T> flinkKafkaConsumer = new FlinkKafkaConsumer<>(Pattern.compile(topicRegex), new MyKafkaDeserializationSchema<>(consumerRecordMapper, typeInformation), props);

        flinkKafkaConsumer
                .setCommitOffsetsOnCheckpoints(true)
                .setStartFromLatest();

        return flinkKafkaConsumer;
    }
}
