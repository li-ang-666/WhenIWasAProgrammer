package com.liang.flink.job;

import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

@LocalConfigFile("patent.yml")
public class PatentJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> dataStream = StreamFactory.create(env);
        Distributor distributor = new Distributor();
        distributor
                .with("company_patent_basic_info_index", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("id")))
                .with("company_index", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("company_id")))
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
        private JdbcTemplate queryTest;
        private JdbcTemplate queryProd;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            queryTest = new JdbcTemplate("427.test");
            queryProd = new JdbcTemplate("451.intellectual_property_info");
            sink = new JdbcTemplate("427.test");
            sink.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            switch (singleCanalBinlog.getTable()) {
                case "company_patent_basic_info_index":
                    parseIndex(singleCanalBinlog);
                    break;
                case "company_index":
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
            String infoIndexId = String.valueOf(columnMap.get("id"));
            String deleteSql = new SQL().DELETE_FROM("company_patent_basic_info_index_split")
                    .WHERE("company_patent_basic_info_index_id = " + SqlUtils.formatValue(infoIndexId))
                    .toString();
            sink.update(deleteSql);
            if (eventType != CanalEntry.EventType.DELETE) {
                String infoId = String.valueOf(columnMap.get("company_patent_info_id"));
                String patentStatus = queryPatentStatus(infoId);
                // info表 和 index表 必须同时都有
                if (patentStatus == null) {
                    return;
                }
                String patentType = String.valueOf(columnMap.get("patent_type"));
                String patentPublishYear = StrUtil.nullToDefault((String) columnMap.get("patent_publish_year"), "0");
                String patentApplicationYear = StrUtil.nullToDefault((String) columnMap.get("patent_application_year"), "0");
                String patentStatusDetail = StrUtil.blankToDefault((String) columnMap.get("patent_latest_legal_review_status"), "其他");
                String patentTitle = String.valueOf(columnMap.get("patent_title"));
                String patentApplicationNumber = String.valueOf(columnMap.get("patent_application_number"));
                String patentAnnounceNumber = String.valueOf(columnMap.get("patent_announce_number"));
                for (String companyId : String.valueOf(columnMap.get("company_ids")).split(";")) {
                    if (!TycUtils.isUnsignedId(companyId)) {
                        continue;
                    }
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("company_patent_basic_info_index_id", infoIndexId);
                    resultMap.put("company_patent_basic_info_id", infoId);
                    resultMap.put("company_id", companyId);
                    resultMap.put("patent_type", patentType);
                    resultMap.put("patent_publish_year", patentPublishYear);
                    resultMap.put("patent_application_year", patentApplicationYear);
                    resultMap.put("patent_status", patentStatus);
                    resultMap.put("patent_status_detail", patentStatusDetail);
                    resultMap.put("patent_title", patentTitle);
                    resultMap.put("patent_application_number", patentApplicationNumber);
                    resultMap.put("patent_announce_number", patentAnnounceNumber);
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
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String companyId = String.valueOf(columnMap.get("company_id"));
            Map<String, AtomicInteger> appYearStatisticMap = new TreeMap<>();
            Map<String, AtomicInteger> pubYearStatisticMap = new TreeMap<>();
            Map<String, AtomicInteger> typeStatisticMap = new TreeMap<>();
            Map<String, AtomicInteger> statusStatisticMap = new TreeMap<>();
            String sql = new SQL()
                    .SELECT("patent_application_year", "patent_publish_year", "patent_type", "patent_status_detail")
                    .FROM("company_patent_basic_info_index_split")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            queryTest.streamQuery(sql, rs -> {
                String appYear = rs.getString(1);
                String pubYear = rs.getString(2);
                String type = rs.getString(3);
                String status = rs.getString(4);
                appYearStatisticMap.compute(appYear + "_" + type, (k, v) -> ObjUtil.defaultIfNull(v, new AtomicInteger(0))).getAndIncrement();
                pubYearStatisticMap.compute(pubYear + "_" + type, (k, v) -> ObjUtil.defaultIfNull(v, new AtomicInteger(0))).getAndIncrement();
                typeStatisticMap.compute(type, (k, v) -> ObjUtil.defaultIfNull(v, new AtomicInteger(0))).getAndIncrement();
                statusStatisticMap.compute(status, (k, v) -> ObjUtil.defaultIfNull(v, new AtomicInteger(0))).getAndIncrement();
            });
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("id", companyId);
            resultMap.put("company_id", companyId);
            resultMap.put("app_year_statistic", JsonUtils.toString(appYearStatisticMap));
            resultMap.put("pub_year_statistic", JsonUtils.toString(pubYearStatisticMap));
            resultMap.put("type_statistic", JsonUtils.toString(typeStatisticMap));
            resultMap.put("status_statistic", JsonUtils.toString(statusStatisticMap));
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String insertSql = new SQL().REPLACE_INTO("company_patent_basic_info_index_split_statistic")
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(insertSql);
        }

        private String queryPatentStatus(String infoId) {
            String sql = new SQL().SELECT("patent_status")
                    .FROM("company_patent_basic_info")
                    .WHERE("id = " + SqlUtils.formatValue(infoId))
                    .toString();
            return queryProd.queryForObject(sql, rs -> rs.getString(1));
        }
    }
}
