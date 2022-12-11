package com.liang.flink.job.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

public class MyKafkaDeserializationSchema<T> implements org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema<T>, Serializable {
    private ConsumerRecordMapper<T> consumerRecordMapper;
    private TypeInformation<T> typeInformation;

    public MyKafkaDeserializationSchema(ConsumerRecordMapper<T> consumerRecordMapper, TypeInformation<T> typeInformation) {
        this.consumerRecordMapper = consumerRecordMapper;
        this.typeInformation = typeInformation;
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return consumerRecordMapper.map(consumerRecord);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        //TypeHint不能定义泛型
        //TypeHint没有实现序列化, 所以还是得传入TypeInformation
        return typeInformation;
    }
}
