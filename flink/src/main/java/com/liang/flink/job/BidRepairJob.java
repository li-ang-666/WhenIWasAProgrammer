package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.bid.BidUtils;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

@Slf4j
@LocalConfigFile("bid-repair.yml")
public class BidRepairJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> e.getColumnMap().get("bid_document_uuid"))
                .addSink(new BidRepairSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidRepairSink")
                .uid("BidRepairSink");
        env.execute("BidRepairJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class BidRepairSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate("448.operating_info");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String bidInfo = String.valueOf(columnMap.get("bid_info"));
            Map<String, Object> parsedColumnMap = BidUtils.parseBidInfo(bidInfo);
            parsedColumnMap.forEach((k, v) -> System.out.println(k + " -> " + v));
        }
    }
}
