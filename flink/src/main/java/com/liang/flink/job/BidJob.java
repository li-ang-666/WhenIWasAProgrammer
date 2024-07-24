package com.liang.flink.job;


import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.bid.BidService;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@LocalConfigFile("bid.yml")
public class BidJob {
    private static final String SINK_RDS = "427.test";
    private static final String SINK_TABlE = "company_bid_parsed_info_patch";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .flatMap(new BidRouter(config))
                .name("BidRouter")
                .uid("BidRouter")
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .keyBy(e -> e)
                .addSink(new BidSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidSink")
                .uid("BidSink");
        env.execute("BidJob");
    }

    @RequiredArgsConstructor
    private static final class BidRouter extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            String uuid = (String) singleCanalBinlog.getColumnMap().get("uuid");
            if (StrUtil.isNotBlank(uuid)) {
                out.collect(uuid);
            }
        }
    }

    @RequiredArgsConstructor
    private static final class BidSink extends RichSinkFunction<String> implements CheckpointedFunction {
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
        public void invoke(String uuid, Context context) {
            Map<String, Object> resultMap = new HashMap<>();
            Map<String, Object> companyBidColumnMap = service.queryCompanyBidAll(uuid);
            if (companyBidColumnMap.isEmpty()) {
                String sql = new SQL().DELETE_FROM(SINK_TABlE)
                        .WHERE("uuid = " + SqlUtils.formatValue(uuid))
                        .toString();
                sink.update(sql);
                return;
            } else {
                resultMap.put("id", companyBidColumnMap.get("id"));
                resultMap.put("uuid", companyBidColumnMap.get("uuid"));
                resultMap.put("title", companyBidColumnMap.get("title"));
                resultMap.put("link", companyBidColumnMap.get("link"));
                resultMap.put("publish_time", companyBidColumnMap.get("publish_time"));
                resultMap.put("type", ObjUtil.defaultIfNull(companyBidColumnMap.get("type"), ""));
                resultMap.put("content_obs_index", "obs://xxx");
                resultMap.put("deleted", companyBidColumnMap.get("deleted"));
            }
            // v1
            Map<String, Object> bidAiV1ColumnMap = service.queryBidAiV1All(uuid);
            resultMap.put("province", StrUtil.blankToDefault((String) bidAiV1ColumnMap.get("province"), ""));
            resultMap.put("city", StrUtil.blankToDefault((String) bidAiV1ColumnMap.get("city"), ""));
            resultMap.put("v1", StrUtil.blankToDefault((String) bidAiV1ColumnMap.get("v1"), ""));
            resultMap.put("v2", StrUtil.blankToDefault((String) bidAiV1ColumnMap.get("v2"), ""));
            resultMap.put("bid_info", StrUtil.blankToDefault((String) bidAiV1ColumnMap.get("bid_info"), "[]"));
            resultMap.put("purchaser", StrUtil.blankToDefault((String) bidAiV1ColumnMap.get("purchaser"), "[]"));
            resultMap.put("winner", StrUtil.blankToDefault((String) bidAiV1ColumnMap.get("winner"), "[]"));
            resultMap.put("winner_amount", StrUtil.blankToDefault((String) bidAiV1ColumnMap.get("winner_amount"), "[]"));
            // v2
            Map<String, Object> bidAiV2ColumnMap = service.queryBidAiV2All(uuid);
            resultMap.put("post_result", StrUtil.blankToDefault((String) bidAiV2ColumnMap.get("post_result"), ""));
            resultMap.put("bidding_unit", StrUtil.blankToDefault((String) bidAiV2ColumnMap.get("bidding_unit"), "[]"));
            resultMap.put("tendering_proxy_agent", StrUtil.blankToDefault((String) bidAiV2ColumnMap.get("tendering_proxy_agent"), "[]"));
            resultMap.put("bid_submission_deadline", StrUtil.blankToDefault((String) bidAiV2ColumnMap.get("bid_submission_deadline"), ""));
            resultMap.put("tender_document_acquisition_deadline", StrUtil.blankToDefault((String) bidAiV2ColumnMap.get("tender_document_acquisition_deadline"), ""));
            resultMap.put("project_number", StrUtil.blankToDefault((String) bidAiV2ColumnMap.get("project_number"), ""));
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String sql = new SQL().REPLACE_INTO(SINK_TABlE)
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
