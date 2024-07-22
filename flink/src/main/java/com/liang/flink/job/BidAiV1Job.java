package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

@LocalConfigFile("bid-ai-v1.yml")
public class BidAiV1Job {
    private static final String SINK_RDS = "427.test";
    private static final String SINK_TABLE = "bid_ai_v1";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new BidAiV1Sink(config))
                .name("BidAiV1Sink")
                .uid("BidAiV1Sink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("BidAiV1Job");
    }

    @RequiredArgsConstructor
    private static final class BidAiV1Sink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate(SINK_RDS);
            sink.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = (String) columnMap.get("id");
            String uuid = (String) columnMap.get("bid_document_uuid");
            String title = (String) columnMap.get("title");
            String bidInfo = StrUtil.blankToDefault((String) columnMap.get("bid_info"), "[]");
            String province = StrUtil.blankToDefault((String) columnMap.get("province"), "");
            String city = StrUtil.blankToDefault((String) columnMap.get("city"), "");
            String firstClassInfoType = StrUtil.blankToDefault((String) columnMap.get("first_class_info_type"), "");
            String secondaryInfoType = StrUtil.blankToDefault((String) columnMap.get("secondary_info_type"), "");
            String bidType = StrUtil.blankToDefault((String) columnMap.get("bid_type"), "");
            HashMap<String, Object> resultMap = new HashMap<>();
            resultMap.put("id", id);
            resultMap.put("uuid", uuid);
            resultMap.put("title", title);
            resultMap.put("province", province);
            resultMap.put("city", city);
            resultMap.put("v1", firstClassInfoType);
            resultMap.put("v2", secondaryInfoType);
            resultMap.put("bid_type", bidType);
            resultMap.put("bid_info", bidInfo);
            resultMap.putAll(BidUtils.parseBidInfo(bidInfo));
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
