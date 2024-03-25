package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
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
            String sql = new SQL().UPDATE("company_bid_parsed_info")
                    .SET("bid_item_num = " + SqlUtils.formatValue(parsedColumnMap.get("item_no")))
                    .SET("bid_contract_num = " + SqlUtils.formatValue(parsedColumnMap.get("contract_no")))
                    .SET("purchaser = " + SqlUtils.formatValue(parsedColumnMap.get("purchasers")))
                    .SET("bid_winner_info_json = " + SqlUtils.formatValue(parsedColumnMap.get("candidates")))
                    .SET("bid_winner = " + SqlUtils.formatValue(parsedColumnMap.get("winners")))
                    .SET("winning_bid_amt_json = " + SqlUtils.formatValue(parsedColumnMap.get("winner_raw_amounts")))
                    .SET("winning_bid_amt_json_clean = " + SqlUtils.formatValue(parsedColumnMap.get("winner_amounts")))
                    .SET("budget_amt_json = " + SqlUtils.formatValue(parsedColumnMap.get("budget_raw_amounts")))
                    .SET("budget_amt_json_clean = " + SqlUtils.formatValue(parsedColumnMap.get("budget_amounts")))
                    .WHERE("bid_uuid = " + SqlUtils.formatValue(columnMap.get("bid_document_uuid")))
                    .toString();
            sink.update(sql);
        }
    }
}
