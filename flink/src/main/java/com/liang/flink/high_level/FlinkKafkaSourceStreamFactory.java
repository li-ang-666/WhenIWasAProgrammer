package com.liang.flink.high_level;

import com.liang.flink.basic.FlinkKafkaSourceFactory;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class FlinkKafkaSourceStreamFactory {
    private FlinkKafkaSourceStreamFactory() {
    }

    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment) {
        FlinkKafkaConsumer<KafkaRecord<BatchCanalBinlog>> kafkaSource = FlinkKafkaSourceFactory.create(BatchCanalBinlog::new);
        return streamEnvironment
                .addSource(kafkaSource)
                .flatMap(new FlatMapFunction<KafkaRecord<BatchCanalBinlog>, SingleCanalBinlog>() {
                    @Override
                    public void flatMap(KafkaRecord<BatchCanalBinlog> kafkaRecord, Collector<SingleCanalBinlog> out) throws Exception {
                        for (SingleCanalBinlog singleCanalBinlog : kafkaRecord.getValue().getSingleCanalBinlogs()) {
                            out.collect(singleCanalBinlog);
                        }
                    }
                });
    }
}
