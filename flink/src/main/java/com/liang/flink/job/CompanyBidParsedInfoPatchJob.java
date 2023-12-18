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
import java.util.stream.Collectors;

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
            // 请求AI
            String mainId = String.valueOf(resultMap.get("main_id"));
            String content = service.getContent(mainId);
            String uuid = String.valueOf(resultMap.get("bid_uuid"));
            List<Map<String, Object>> queryResult = service.query(uuid);
            List<Map<String, Object>> result = !queryResult.isEmpty() ? queryResult : service.post(content, uuid);
            if (log.isDebugEnabled()) {
                log.debug("AI return: {} -> {}", uuid, JsonUtils.toString(result));
            }
            // owner 招标方
            String sourceOwner = String.valueOf(columnMap.get("purchaser"));
            List<Map<String, Object>> newOwner = service.newJson(sourceOwner);
            // agent 代理方
            List<Map<String, Object>> newAgent;
            newAgent = result.stream()
                    .filter(e -> e.containsValue("proxy_unit") && e.containsKey("company_gid") && e.containsKey("clean_word"))
                    .map(e -> new HashMap<String, Object>() {{
                        put("gid", e.get("company_gid"));
                        put("name", e.get("clean_word"));
                    }})
                    .collect(Collectors.toList());
            if (newAgent.isEmpty()) {
                String sourceAgent = String.valueOf(columnMap.get("proxy_unit"));
                newAgent = service.newAgentJson(sourceAgent);
            }
            // tenderer 投标方
            List<Map<String, Object>> newTenderer = result.stream()
                    .filter(e -> e.containsValue("tenderer_unit") && e.containsKey("company_gid") && e.containsKey("clean_word"))
                    .map(e -> new HashMap<String, Object>() {{
                        put("gid", e.get("company_gid"));
                        put("name", e.get("clean_word"));
                    }})
                    .collect(Collectors.toList());
            // candidate 候选方
            String sourceCandidate = String.valueOf(columnMap.get("bid_winner_info_json"));
            List<Map<String, Object>> newCandidate = service.newJson(sourceCandidate);
            // winner 中标方
            String sourceWinner = String.valueOf(columnMap.get("bid_winner"));
            List<Map<String, Object>> newWinner = service.newJson(sourceWinner);
            // winner amt 中标金额
            String sourceWinnerAmt = String.valueOf(columnMap.get("winning_bid_amt_json_clean"));
            List<Map<String, Object>> newWinnerAmt = service.newJson(sourceWinnerAmt);
            // put & sink
            resultMap.put("party_a", JsonUtils.toString(service.deduplicateGidAndName(newOwner)));
            resultMap.put("agent", JsonUtils.toString(service.deduplicateGidAndName(newAgent)));
            resultMap.put("tenderer", JsonUtils.toString(service.deduplicateGidAndName(newTenderer)));
            resultMap.put("candidate", JsonUtils.toString(service.deduplicateGidAndName(newCandidate)));
            resultMap.put("winner", JsonUtils.toString(newWinner));
            resultMap.put("winner_amt", JsonUtils.toString(newWinnerAmt));
            for (Map<String, Object> map : result) {
                if (map.containsValue("item_no")) {
                    resultMap.put("item_no", map.getOrDefault("clean_word", ""));
                } else if (map.containsValue("bid_deadline")) {
                    resultMap.put("bid_deadline", map.getOrDefault("clean_word", null));
                } else if (map.containsValue("bid_download_deadline")) {
                    resultMap.put("bid_download_deadline", map.getOrDefault("clean_word", null));
                }
            }
            service.sink(resultMap);
        }
    }
}
