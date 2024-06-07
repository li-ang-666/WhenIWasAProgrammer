package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

@LocalConfigFile("patent.yml")
public class PatentJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> dataStream = StreamFactory.create(env);
        Distributor distributor = new Distributor();
        distributor
                .with("company_patent_basic_info_index", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("id")))
                .with("company_patent_basic_info_index_split", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("company_id")));
        dataStream
                .keyBy(distributor)
                .addSink(new PatentSink(config))
                .name("PatentSink")
                .uid("PatentSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("PatentJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class PatentSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            //sink = new JdbcTemplate("451.intellectual_property_info");
            sink = new JdbcTemplate("427.test");
            sink.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            switch (singleCanalBinlog.getTable()) {
                case "company_patent_basic_info_index":
                    parseIndex(singleCanalBinlog);
                    break;
                case "company_patent_basic_info_index_split":
                    parseIndexSplit(singleCanalBinlog);
                    break;
                default:
                    break;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
        }

        @Override
        public void finish() {
            flush();
        }

        @Override
        public void close() {
            flush();
        }

        private void flush() {
            sink.flush();
        }

        private void parseIndex(SingleCanalBinlog singleCanalBinlog) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            CanalEntry.EventType eventType = singleCanalBinlog.getEventType();
            String id = String.valueOf(columnMap.get("id"));
            String deleteSql = new SQL().DELETE_FROM("company_patent_basic_info_index_split")
                    .WHERE("company_patent_basic_info_index_id = " + SqlUtils.formatValue(id))
                    .toString();
            sink.update(deleteSql);
            if (eventType != CanalEntry.EventType.DELETE) {
                for (String companyId : String.valueOf(columnMap.get("company_ids")).split(";")) {
                    if (!TycUtils.isUnsignedId(companyId)) {
                        continue;
                    }
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("company_patent_basic_info_index_id", String.valueOf(columnMap.get("id")));
                    resultMap.put("company_patent_basic_info_id", String.valueOf(columnMap.get("company_patent_info_id")));
                    resultMap.put("company_id", String.valueOf(companyId));
                    resultMap.put("patent_type", String.valueOf(columnMap.get("patent_type")));
                    resultMap.put("patent_publish_year", StrUtil.nullToDefault((String) columnMap.get("patent_publish_year"), "0"));
                    resultMap.put("patent_application_year", StrUtil.nullToDefault((String) columnMap.get("patent_application_year"), "0"));
                    resultMap.put("patent_status", StrUtil.blankToDefault((String) columnMap.get("patent_latest_legal_review_status"), "其他"));
                    resultMap.put("patent_title", String.valueOf(columnMap.get("patent_title")));
                    resultMap.put("patent_application_number", String.valueOf(columnMap.get("patent_application_number")));
                    resultMap.put("patent_announce_number", String.valueOf(columnMap.get("patent_announce_number")));
                    Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
                    String insertSql = new SQL().INSERT_INTO("company_patent_basic_info_index_split")
                            .INTO_COLUMNS(insert.f0)
                            .INTO_VALUES(insert.f1)
                            .toString();
                    sink.update(insertSql);
                }
            }
        }

        private void parseIndexSplit(SingleCanalBinlog singleCanalBinlog) {
        }
    }
}
