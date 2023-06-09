package com.liang.flink.high.level.api;

import com.liang.flink.basic.KafkaSourceFactory;
import com.liang.flink.basic.RichMapMonitor;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KafkaSourceStreamFactory {
    private KafkaSourceStreamFactory() {
    }

    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment, int parallel) {
        KafkaSource<KafkaRecord<BatchCanalBinlog>> kafkaSource = KafkaSourceFactory.create(BatchCanalBinlog::new);
        String name = "KafkaSource";
        return streamEnvironment
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), name)
                .setParallelism(parallel)
                .map(new RichMapMonitor<>())
                .setParallelism(1)
                .flatMap(new FlatMapFunction<KafkaRecord<BatchCanalBinlog>, SingleCanalBinlog>() {
                    @Override
                    public void flatMap(KafkaRecord<BatchCanalBinlog> kafkaRecord, Collector<SingleCanalBinlog> out) throws Exception {
                        for (SingleCanalBinlog singleCanalBinlog : kafkaRecord.getValue().getSingleCanalBinlogs()) {
                            out.collect(singleCanalBinlog);
                        }
                    }
                }).setParallelism(1);
    }
}
