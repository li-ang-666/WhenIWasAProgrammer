package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.company.bid.parsed.info.patch.CompanyBidParsedInfoPatchService;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
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
                .addSink(new CompanyBidParsedInfoPatchSink(config)).name("CompanyBidParsedInfoPatchSink").setParallelism(config.getFlinkConfig().getOtherParallel());
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
            // 上游删除
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE || !"0".equals(String.valueOf(columnMap.get("is_deleted")))) {
                service.delete(String.valueOf(columnMap.get("id")));
                return;
            }
            // 无效content, 删除
            String content = service.getContent(String.valueOf(columnMap.get("main_id")));
            if (!TycUtils.isValidName(content)) {
                service.delete(String.valueOf(columnMap.get("id")));
                return;
            }
            // 先请求RDS
            String uuid = String.valueOf(columnMap.get("bid_uuid"));
            List<Map<String, Object>> queryResult = service.query(uuid);
            // 再请求AI
            List<Map<String, Object>> result = !queryResult.isEmpty() ? queryResult : service.post(content, uuid);
            if (log.isDebugEnabled()) log.debug("AI return: {} -> {}", uuid, JsonUtils.toString(result));
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
            // mention 其他被提及
            List<Map<String, Object>> mention = service.getMention(String.valueOf(columnMap.get("main_id")));
            // 去重
            HashMap<String, Object> resultMap = new HashMap<>(columnMap);
            // 1、
            List<Map<String, Object>> finalPartyA = service.deduplicateGidAndName(newOwner);
            String partyAString = JsonUtils.toString(finalPartyA);
            resultMap.put("party_a", partyAString);
            // 2、
            List<Map<String, Object>> finalAgent = service.deduplicateGidAndName(newAgent);
            finalAgent.removeIf(e -> partyAString.contains(String.valueOf(e.get("gid"))));
            String agentString = JsonUtils.toString(finalAgent);
            resultMap.put("agent", agentString);
            // 3、
            List<Map<String, Object>> finalTenderer = service.deduplicateGidAndName(newTenderer);
            finalTenderer.removeIf(e -> partyAString.contains(String.valueOf(e.get("gid"))) || agentString.contains(String.valueOf(e.get("gid"))));
            String tendererString = JsonUtils.toString(finalTenderer);
            resultMap.put("tenderer", tendererString);
            // 4、
            List<Map<String, Object>> finalCandidate = service.deduplicateGidAndName(newCandidate);
            finalCandidate.removeIf(e -> partyAString.contains(String.valueOf(e.get("gid"))) || agentString.contains(String.valueOf(e.get("gid"))));
            String candidateString = JsonUtils.toString(finalCandidate);
            resultMap.put("candidate", candidateString);
            // 5.1、
            newWinner.removeIf(e -> partyAString.contains(String.valueOf(e.get("gid"))) || agentString.contains(String.valueOf(e.get("gid"))));
            String winnerString = JsonUtils.toString(newWinner);
            resultMap.put("winner", winnerString);
            // 5.2、
            if (newWinnerAmt.size() != newWinner.size()) {
                newWinnerAmt.clear();
            }
            resultMap.put("winner_amt", JsonUtils.toString(newWinnerAmt));
            // 6、
            mention.removeIf(e ->
                    partyAString.contains(String.valueOf(e.get("gid")))
                            || agentString.contains(String.valueOf(e.get("gid")))
                            || tendererString.contains(String.valueOf(e.get("gid")))
                            || candidateString.contains(String.valueOf(e.get("gid")))
                            || winnerString.contains(String.valueOf(e.get("gid")))
            );
            String mentionString = JsonUtils.toString(mention);
            resultMap.put("mention", mentionString);
            // 编号 & 时间
            for (Map<String, Object> map : result) {
                if (map.containsValue("item_no")) {
                    resultMap.put("item_no", map.getOrDefault("clean_word", ""));
                } else if (map.containsValue("bid_deadline")) {
                    String bidDeadline = String.valueOf(map.get("clean_word"));
                    resultMap.put("bid_deadline", bidDeadline.matches("\\d{4}-\\d{2}-\\d{2}") ? bidDeadline : null);
                } else if (map.containsValue("bid_download_deadline")) {
                    String bidDownloadDeadline = String.valueOf(map.get("clean_word"));
                    resultMap.put("bid_download_deadline", bidDownloadDeadline.matches("\\d{4}-\\d{2}-\\d{2}") ? bidDownloadDeadline : null);
                }
            }
            // 格式化省、市
            Tuple2<String, String> tp2 = service.formatCode(String.valueOf(columnMap.get("bid_province")), String.valueOf(columnMap.get("bid_city")));
            resultMap.put("province", tp2.f0);
            resultMap.put("city", tp2.f1);
            // 搜索
            String bidPublishTime = columnMap.get("bid_publish_time") + "suffix";
            String substring = bidPublishTime.substring(0, 4);
            resultMap.put("publish_year", StringUtils.isNumeric(substring) ? substring : null);
            String roles = "招采方:" + finalPartyA.stream().filter(e -> TycUtils.isUnsignedId(e.get("gid"))).map(e -> String.valueOf(e.get("gid"))).collect(Collectors.joining(","))
                    + ";代理方:" + finalAgent.stream().filter(e -> TycUtils.isUnsignedId(e.get("gid"))).map(e -> String.valueOf(e.get("gid"))).collect(Collectors.joining(","))
                    + ";投标方:" + finalTenderer.stream().filter(e -> TycUtils.isUnsignedId(e.get("gid"))).map(e -> String.valueOf(e.get("gid"))).collect(Collectors.joining(","))
                    + ";中标候选人:" + finalCandidate.stream().filter(e -> TycUtils.isUnsignedId(e.get("gid"))).map(e -> String.valueOf(e.get("gid"))).collect(Collectors.joining(","))
                    + ";中标方:" + newWinner.stream().filter(e -> TycUtils.isUnsignedId(e.get("gid"))).map(e -> String.valueOf(e.get("gid"))).collect(Collectors.joining(","))
                    + ";被提及:" + mention.stream().filter(e -> TycUtils.isUnsignedId(e.get("gid"))).map(e -> String.valueOf(e.get("gid"))).collect(Collectors.joining(","));
            resultMap.put("roles", roles);
            service.sink(resultMap);
        }
    }
}
