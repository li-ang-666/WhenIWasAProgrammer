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

import java.util.*;

@Slf4j
@SuppressWarnings("unchecked")
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

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            source = new JdbcTemplate("427.test");
            sink = new JdbcTemplate("427.test");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            // read map
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = String.valueOf(columnMap.get("id"));
            // delete mysql
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE) {
                String deleteSql = new SQL()
                        .DELETE_FROM(SINK_TABLE)
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                sink.update(deleteSql);
            }
            String uuid = String.valueOf(columnMap.get("uuid"));
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

        private Map<String, Object> parseJson(String json) {
            Map<String, Object> columnMap = new HashMap<>();
            List<Map<String, Object>> entities = Optional.ofNullable(json)
                    .map(JsonUtils::parseJsonObj)
                    .map(e -> e.get("result"))
                    .map(e -> ((Map<String, Object>) e).get("entities"))
                    .map(e -> (List<Map<String, Object>>) e)
                    .orElse(new ArrayList<>());
            for (Map<String, Object> entity : entities) {

            }

            return null;
        }
    }
}
