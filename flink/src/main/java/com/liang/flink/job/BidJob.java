package com.liang.flink.job;


import cn.hutool.http.HttpUtil;
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

@Slf4j
@SuppressWarnings("unchecked")
@LocalConfigFile("bid.yml")
public class BidJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> e.getColumnMap().get("id"))
                .addSink(new BidSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidSink")
                .uid("BidSink");
        env.execute("BidJob");
    }

    @RequiredArgsConstructor
    private static final class BidSink extends RichSinkFunction<SingleCanalBinlog> {
        private static final int TIMEOUT = 1000 * 60;
        private static final String URL = "https://bid.tianyancha.com/bid_rank";
        private static final String SINK_TABlE = "company_bid_plus";
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate("427.test");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            // read map
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            columnMap.entrySet().removeIf(e -> e.getValue() == null);
            String id = String.valueOf(columnMap.get("id"));
            String uuid = String.valueOf(columnMap.get("uuid"));
            String content = String.valueOf(columnMap.get("content"));
            // delete mysql
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE) {
                String deleteSql = new SQL().DELETE_FROM(SINK_TABlE)
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                sink.update(deleteSql);
                return;
            }
            // read AI
            String entitiesString = Optional.ofNullable(doPost(uuid, content))
                    .map(JsonUtils::parseJsonObj)
                    .map(postColumnMap -> postColumnMap.get("result"))
                    .map(result -> (Map<String, Object>) result)
                    .map(resultMap -> resultMap.get("entities"))
                    .map(JsonUtils::toString)
                    .orElse("[]");
            // write map
            columnMap.put("post_result", entitiesString);
            // write mysql
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String insertSql = new SQL().REPLACE_INTO(SINK_TABlE)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(insertSql);
        }

        private String doPost(String uuid, String content) {
            do {
                try {
                    Map<String, Object> paramMap = Collections.singletonMap("text", content);
                    return HttpUtil.post(URL, paramMap, TIMEOUT);
                } catch (Exception e) {
                    log.warn("post error, uuid: {}, exception: {} {}", uuid, e.getClass().getName(), e.getMessage());
                }
            } while (true);
        }
    }
}
