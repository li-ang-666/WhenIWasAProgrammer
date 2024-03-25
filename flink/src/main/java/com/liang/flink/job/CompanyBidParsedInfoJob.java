package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.liang.flink.project.bid.BidUtils.parseBidInfo;

@Slf4j
@LocalConfigFile("company-bid-parsed-info.yml")
public class CompanyBidParsedInfoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new CompanyBidParsedInfoSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("CompanyBidParsedInfoSink")
                .uid("CompanyBidParsedInfoSink");
        env.execute("CompanyBidParsedInfoJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class CompanyBidParsedInfoSink extends RichSinkFunction<SingleCanalBinlog> {
        private static final String SINK_TABLE = "company_bid_parsed_info";
        private final Config config;
        private JdbcTemplate source;
        private JdbcTemplate sink;
        private JdbcTemplate companyBase435;
        private JdbcTemplate semanticAnalysis069;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            source = new JdbcTemplate("427.test");
            sink = new JdbcTemplate("427.test");
            companyBase435 = new JdbcTemplate("435.company_base");
            semanticAnalysis069 = new JdbcTemplate("069.semantic_analysis");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            // read map
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = String.valueOf(columnMap.get("id"));
            String uuid = String.valueOf(columnMap.get("uuid"));
            // delete mysql
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE) {
                String deleteSql = new SQL()
                        .DELETE_FROM(SINK_TABLE)
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                sink.update(deleteSql);
            }
            Map<String, Object> resultMap = new HashMap<>();
            //
            String postResult;
            if (columnMap.containsKey("post_result")) {
                postResult = String.valueOf(columnMap.get("post_result"));
            } else {
                String querySql = new SQL()
                        .SELECT("post_result")
                        .FROM("company_bid_plus")
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                String queryResult = source.queryForObject(querySql, rs -> rs.getString(1));
                postResult = (queryResult != null) ? queryResult : "{}";
            }
            Map<String, Object> postResultColumnMap = parseJson(postResult);
        }

        //@Override
        //public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
        //    Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        //    String bidInfo = String.valueOf(columnMap.get("bid_info"));
        //    Map<String, Object> parsedColumnMap = parseBidInfo(bidInfo);
        //    for (Map.Entry<String, Object> entry : parsedColumnMap.entrySet()) {
        //        String key = entry.getKey();
        //        Object value = entry.getValue();
        //        System.out.println(key + " -> " + value);
        //    }
        //}

        private Map<String, Object> parseJson(String json) {
            Map<String, Object> columnMap = new LinkedHashMap<>();
            Map<String, Object> result = Optional.ofNullable(json)
                    .map(JsonUtils::parseJsonObj)
                    .map(e -> e.get("result"))
                    .map(e -> (Map<String, Object>) e)
                    .orElseGet(HashMap::new);
            // simple column
            columnMap.put("bid_province", result.getOrDefault("province", ""));
            columnMap.put("bid_city", result.getOrDefault("city", ""));
            columnMap.put("public_info_lv1", result.getOrDefault("first_class_info_type", ""));
            columnMap.put("public_info_lv2", result.getOrDefault("secondary_info_type", ""));
            columnMap.put("bid_type", result.getOrDefault("bid_type", ""));
            columnMap.put("is_dirty", result.getOrDefault("is_dirty", "0"));
            columnMap.putAll(parseBidInfo(String.valueOf(result.getOrDefault("bid_info", "[]"))));
            return columnMap;
        }
    }
}
