package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.company.bid.parsed.info.patch.CompanyBidParsedInfoPatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@LocalConfigFile("company-bid-parsed-info-patch.yml")
public class CompanyBidParsedInfoPatchJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new CompanyBidParsedInfoPatchSink(config)).setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("CompanyBidParsedInfoPatchJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class CompanyBidParsedInfoPatchSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private CompanyBidParsedInfoPatchService service;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new CompanyBidParsedInfoPatchService();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            HashMap<String, Object> resultMap = new HashMap<>(columnMap);
            // 请求接口
            String uuid = String.valueOf(resultMap.get("bid_uuid"));
            String title = String.valueOf(resultMap.get("bid_title"));
            String content = String.valueOf(resultMap.get("bid_content"));
            // owner 招标方
            String sourceOwner = String.valueOf(columnMap.get("purchaser"));
            List<Map<String, Object>> owner = service.newJson(sourceOwner);
            // agent 代理方
            String sourceAgent = String.valueOf(columnMap.get("proxy_unit"));
            List<Map<String, Object>> agent = service.newAgentJson(sourceAgent);
            // tenderer 投标方
            // ...
            // candidate 候选方
            String sourceCandidate = String.valueOf(columnMap.get("bid_winner_info_json"));
            List<Map<String, Object>> candidate = service.newJson(sourceCandidate);
            // winner 中标方
            String sourceWinner = String.valueOf(columnMap.get("bid_winner"));
            List<Map<String, Object>> winner = service.newJson(sourceWinner);
            // winner amt 中标金额
            String sourceWinnerAmt = String.valueOf(columnMap.get("winning_bid_amt_json_clean"));
            List<Map<String, Object>> winnerAmt = service.newJson(sourceWinnerAmt);
            // put & sink
            resultMap.put("owner", JsonUtils.toString(service.deduplicateGidAndName(owner)));
            resultMap.put("agent", JsonUtils.toString(service.deduplicateGidAndName(agent)));
            resultMap.put("tenderer", "[]");
            resultMap.put("candidate", JsonUtils.toString(service.deduplicateGidAndName(candidate)));
            resultMap.put("winner", JsonUtils.toString(winner));
            resultMap.put("winner_amt", JsonUtils.toString(winnerAmt));
            resultMap.put("item_no", "");
            resultMap.put("bid_deadline", null);
            resultMap.put("bid_download_deadline", null);
            service.sink(resultMap);
        }
    }
}
