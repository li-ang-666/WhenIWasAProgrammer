package com.liang.flink.job;


import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import com.vladsch.flexmark.html2md.converter.FlexmarkHtmlConverter;
import com.vladsch.flexmark.util.data.MutableDataSet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@LocalConfigFile("bid-to-cloud.yml")
public class BidToCloudJob {
    private static final String SINK_RDS = "427.test";
    private static final String SINK_TABlE = "company_bid";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> (String) e.getColumnMap().get("id"))
                .addSink(new BidToCloudSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidToCloudSink")
                .uid("BidToCloudSink");
        env.execute("BidToCloudJob");
    }

    @RequiredArgsConstructor
    private static final class BidToCloudSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
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
            HashMap<String, Object> resultMap = new HashMap<>();
            resultMap.put("id", columnMap.get("id"));
            resultMap.put("uuid", columnMap.get("uuid"));
            resultMap.put("content", htmlToMd((String) columnMap.get("content")));
            resultMap.put("deleted", columnMap.get("deleted"));
            resultMap.put("type", columnMap.get("type"));
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String sql = new SQL().REPLACE_INTO(SINK_TABlE)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(sql);
        }

        private String htmlToMd(String html) {
            MutableDataSet options = new MutableDataSet();
            FlexmarkHtmlConverter converter = FlexmarkHtmlConverter.builder(options).build();
            return converter.convert(html);
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
