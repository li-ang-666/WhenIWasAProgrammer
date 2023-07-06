package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
public class DataConcatJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = new String[]{"data-concat.yml"};
        StreamExecutionEnvironment streamEnvironment = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(streamEnvironment);
        stream
                .rebalance()
                .addSink(new DataConcatRichSinkFunction(ConfigUtils.getConfig()))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("DataConcatHbaseSink");
        streamEnvironment.execute("DataConcatJob");
    }

    @Slf4j
    private static class DataConcatRichSinkFunction extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DataUpdateService<HbaseOneRow> service;
        private HbaseTemplate hbase;

        private DataConcatRichSinkFunction(Config config) {
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
            hbase = new HbaseTemplate("hbaseSink");
            hbase.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) throws Exception {
            List<HbaseOneRow> result = service.invoke(singleCanalBinlog);
            if (result == null || result.isEmpty()) {
                return;
            }
            if (log.isDebugEnabled()) {
                for (HbaseOneRow hbaseOneRow : result) {
                    String rowKey = hbaseOneRow.getRowKey();
                    Map<String, Object> columnMap = new TreeMap<>(hbaseOneRow.getColumnMap());
                    StringBuilder builder = new StringBuilder();
                    builder.append(String.format("\nrowKey: %s", rowKey));
                    for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                        builder.append(String.format("\n%s -> %s", entry.getKey(), entry.getValue()));
                    }
                    log.debug("print before sink: {}", builder);
                }
            }
            for (HbaseOneRow hbaseOneRow : result) {
                hbase.update(hbaseOneRow);
            }
        }
    }
}
