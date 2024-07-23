package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
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
import com.liang.flink.project.bid.BidService;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

@LocalConfigFile("bid-ai-v2.yml")
public class BidAiV2Job {
    private static final String SINK_RDS = "427.test";
    private static final String SINK_TABLE = "bid_ai_v2";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> (String) e.getColumnMap().get("id"))
                .addSink(new BidAiV2Sink(config))
                .name("BidAiV2Sink")
                .uid("BidAiV2Sink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("BidAiV2Job");
    }

    @RequiredArgsConstructor
    private static final class BidAiV2Sink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;
        private BidService service;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate(SINK_RDS);
            sink.enableCache();
            service = new BidService();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = (String) columnMap.get("id");
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE) {
                String sql = new SQL().DELETE_FROM(SINK_TABLE)
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                sink.update(sql);
                return;
            }
            String uuid = StrUtil.blankToDefault((String) columnMap.get("uuid"), "");
            String title = StrUtil.blankToDefault((String) columnMap.get("title"), "");
            String content = StrUtil.blankToDefault((String) columnMap.get("content"), "");
            String type = StrUtil.blankToDefault((String) columnMap.get("type"), "");
            String postResult = service.post(uuid, title, content, type);
            Map<String, Object> postMap = JsonUtils.parseJsonObj(postResult);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("id", id);
            resultMap.put("uuid", uuid);
            resultMap.put("title", title);
            resultMap.put("content", content);
            resultMap.put("type", type);
            resultMap.put("bidding_unit", "");
            resultMap.put("tendering_proxy_agent", "");
            resultMap.put("bid_submission_deadline", "");
            resultMap.put("tender_document_acquisition_deadline", "");
            resultMap.put("project_number", "");
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String sql = new SQL().REPLACE_INTO(SINK_TABLE)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(sql);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            sink.flush();
        }

        @Override
        public void finish() {
            sink.flush();
        }

        @Override
        public void close() {
            sink.flush();
        }
    }
}
