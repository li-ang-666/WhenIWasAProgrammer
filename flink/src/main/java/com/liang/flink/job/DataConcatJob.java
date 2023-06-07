package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.config.FlinkSource;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.KafkaSourceStreamFactory;
import com.liang.flink.high.level.api.RepairSourceStreamFactory;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DataConcatJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = new String[]{"data-concat-config.yml"};
        StreamExecutionEnvironment streamEnvironment = StreamEnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = config.getFlinkSource() == FlinkSource.Repair ?
                RepairSourceStreamFactory.create(streamEnvironment) :
                KafkaSourceStreamFactory.create(streamEnvironment);
        stream
                .rebalance()
                .map(new DataConcatRichMapFunction(ConfigUtils.getConfig()))
                .addSink(new DataConcatRichSinkFunction(ConfigUtils.getConfig()))
                .uid("DataConcatHbaseSink")
                .name("DataConcatHbaseSink");
        streamEnvironment.execute();
    }

    //核心处理类 dataConcatService.invoke(singleCanalBinlog)
    private static class DataConcatRichMapFunction extends RichMapFunction<SingleCanalBinlog, List<HbaseOneRow>> {
        private final Config config;
        private DataUpdateService<HbaseOneRow> service;

        private DataConcatRichMapFunction(Config config) throws Exception {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            DataUpdateContext<HbaseOneRow> dataUpdateContext = new DataUpdateContext<HbaseOneRow>
                    ("com.liang.flink.project.data.concat.impl")
                    .addClass("RestrictConsumptionSplitIndex")
                    .addClass("JudicialAssistanceIndex")
                    .addClass("RestrictedOutboundIndex")
                    .addClass("EquityPledgeReinvest")
                    .addClass("EquityPledgeDetail")
                    .addClass("CompanyBranch");
            service = new DataUpdateService<>(dataUpdateContext);
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
            hbase = new HbaseTemplate("hbaseSink");
        }

        @Override
        public void invoke(List<HbaseOneRow> input, Context context) throws Exception {
            if (input == null || input.size() == 0) {
                return;
            }
            for (HbaseOneRow hbaseOneRow : input) {
                String rowKey = hbaseOneRow.getRowKey();
                Map<String, Object> columnMap = new HashMap<>(hbaseOneRow.getColumnMap());
                StringBuilder builder = new StringBuilder();
                builder.append(String.format("\nrowKey: %s", rowKey));
                for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                    builder.append(String.format("\n%s -> %s", entry.getKey(), entry.getValue()));
                }
                log.info("{}", builder);
            }
            for (HbaseOneRow hbaseOneRow : input) {
                hbase.upsert(hbaseOneRow);
            }
        }
    }
}
