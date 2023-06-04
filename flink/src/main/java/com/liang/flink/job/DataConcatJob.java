package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.config.FlinkSource;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high_level.KafkaSourceStreamFactory;
import com.liang.flink.high_level.RepairSourceStreamFactory;
import com.liang.flink.service.DataConcatService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Random;

@Slf4j
public class DataConcatJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnvironment = StreamEnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = config.getFlinkSource() == FlinkSource.Repair ?
                RepairSourceStreamFactory.create(streamEnvironment) :
                KafkaSourceStreamFactory.create(streamEnvironment);
        stream
                .keyBy(new DataConcatKeySelector())
                .map(new DataConcatRichMapFunction(ConfigUtils.getConfig()))
                .addSink(new DataConcatRichSinkFunction(ConfigUtils.getConfig()))
                .uid("DataConcatHbaseSink")
                .name("DataConcatHbaseSink");
        streamEnvironment.execute();
    }

    //KafkaRecord<BatchCanalBinlog> -拆包-> SingleCanalBinlog
    private static class DataConcatFlatMapFunction implements FlatMapFunction<KafkaRecord<BatchCanalBinlog>, SingleCanalBinlog> {
        @Override
        public void flatMap(KafkaRecord<BatchCanalBinlog> kafkaRecord, Collector<SingleCanalBinlog> out) throws Exception {
            BatchCanalBinlog batchCanalBinlog = kafkaRecord.getValue();
            for (SingleCanalBinlog singleCanalBinlog : batchCanalBinlog.getSingleCanalBinlogs()) {
                out.collect(singleCanalBinlog);
            }
        }
    }

    //keyBy(id)
    private static class DataConcatKeySelector implements KeySelector<SingleCanalBinlog, String> {
        private final Random random = new Random();

        @Override
        public String getKey(SingleCanalBinlog singleCanalBinlog) throws Exception {
            return String.valueOf(
                    singleCanalBinlog.getColumnMap()
                            .getOrDefault("id", random.nextInt())
            );
        }
    }

    //核心处理类 dataConcatService.invoke(singleCanalBinlog)
    private static class DataConcatRichMapFunction extends RichMapFunction<SingleCanalBinlog, List<HbaseOneRow>> {
        private final Config config;
        private DataConcatService service;

        private DataConcatRichMapFunction(Config config) throws Exception {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            service = new DataConcatService();
        }

        @Override
        public List<HbaseOneRow> map(SingleCanalBinlog singleCanalBinlog) throws Exception {
            return service.invoke(singleCanalBinlog);
        }
    }

    //HbaseSink
    private static class DataConcatRichSinkFunction extends RichSinkFunction<List<HbaseOneRow>> {
        private final Config config;
        private HbaseTemplate hbase;

        private DataConcatRichSinkFunction(Config config) throws Exception {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            hbase = new HbaseTemplate("test");
        }

        @Override
        public void invoke(List<HbaseOneRow> input, Context context) throws Exception {
            if (input == null || input.size() == 0) {
                return;
            }
//            for (HbaseOneRow hbaseOneRow : input) {
//                String rowKey = hbaseOneRow.getRowKey();
//                Map<String, Object> columnMap = new TreeMap<>(hbaseOneRow.getColumnMap());
//                StringBuilder builder = new StringBuilder();
//                builder.append(String.format("\nrowKey: %s", rowKey));
//                for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
//                    builder.append(String.format("\n%s -> %s", entry.getKey(), entry.getValue()));
//                }
//                log.info("{}", builder);
//            }
            for (HbaseOneRow hbaseOneRow : input) {
                hbase.upsert(hbaseOneRow);
            }
        }
    }
}
