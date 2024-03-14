package com.liang.flink.job;


import cn.hutool.http.HttpUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
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

import java.util.HashMap;
import java.util.Map;

@Slf4j
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
        private static final String URL = "http://10.99.199.173:10040/linking_yuqing_rank";
        private static final String SINK_TABlE = "company_bid_with_post_result";
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate("448.operating_info");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            // read map
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = String.valueOf(columnMap.get("id"));
            String uuid = String.valueOf(columnMap.get("uuid"));
            String content = String.valueOf(columnMap.get("content"));
            // delete
            String deleteSql = new SQL().DELETE_FROM(SINK_TABlE)
                    .WHERE("id = " + SqlUtils.formatValue(id))
                    .toString();
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE) {
                sink.update(deleteSql);
                return;
            }
            // write map
            columnMap.put("post_result", doPost(content, uuid));
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String insertSql = new SQL().REPLACE_INTO(SINK_TABlE)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(deleteSql, insertSql);
        }

        private String doPost(String content, String uuid) {
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("text", content);
            paramMap.put("bid_uuid", uuid);
            return HttpUtil.post(URL, paramMap, TIMEOUT);
        }
    }
}
