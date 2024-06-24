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
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@LocalConfigFile("equity-direct.yml")
public class EquityDirectJob {
    private static final String ON_DUPLICATE_KEY_UPDATE = SqlUtils.onDuplicateKeyUpdate(
            "company_id",
            "company_name",
            "shareholder_type",
            "shareholder_type_show",
            "shareholder_name_id",
            "shareholder_id",
            "shareholder_name",
            "equity_ratio",
            "data_source",
            "investment_start_time",
            "update_time",
            "company_id_invested",
            "tyc_unique_entity_id_invested",
            "tyc_unique_entity_name_invested",
            "investor_identity_type",
            "company_id_investor",
            "tyc_unique_entity_id_investor",
            "tyc_unique_entity_name_investor",
            "equity_amount",
            "equity_amount_currency",
            "equity_relation_validation_year",
            "reference_pt_year"
    );
    private static final String QUERY_RDS = "435.company_base";
    private static final String QUERY_TABLE = "shareholder_investment_relation_info_tmp";

    private static final String SINK_RDS = "427.test";
    private static final String SINK_TABLE = "company_equity_relation_details_tmp";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .flatMap(new EquityDirectFlatMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityDirectFlatMapper")
                .uid("EquityDirectFlatMapper")
                .keyBy(e -> e)
                .addSink(new EquityDirectSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityDirectSink")
                .uid("EquityDirectSink");
        env.execute("EquityDirectJob");
    }

    @RequiredArgsConstructor
    private static final class EquityDirectFlatMapper extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;
        private JdbcTemplate trigger;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            trigger = new JdbcTemplate(QUERY_RDS);
            trigger.enableCache(2000, 128);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            String table = singleCanalBinlog.getTable();
            switch (table) {
                case QUERY_TABLE:
                    out.collect((String) singleCanalBinlog.getColumnMap().get("id"));
                    break;
                case "company_human_relation":
                    Set<Tuple2<String, String>> tuple2s = new HashSet<>();
                    Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
                    tuple2s.add(Tuple2.of((String) beforeColumnMap.get("company_graph_id"), (String) beforeColumnMap.get("human_graph_id")));
                    Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
                    tuple2s.add(Tuple2.of((String) afterColumnMap.get("company_graph_id"), (String) afterColumnMap.get("human_graph_id")));
                    for (Tuple2<String, String> tuple2 : tuple2s) {
                        for (String id : queryIds(tuple2)) {
                            out.collect(id);
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        private List<String> queryIds(Tuple2<String, String> companyIdAndHumanNameId) {
            ArrayList<String> ids = new ArrayList<>();
            String companyGraphId = companyIdAndHumanNameId.f0;
            String humanGraphId = companyIdAndHumanNameId.f1;
            if (StrUtil.equalsAny("0", companyGraphId, humanGraphId)) {
                return ids;
            }
            String sql = new SQL().SELECT("id")
                    .FROM(QUERY_TABLE)
                    .WHERE("company_id = " + SqlUtils.formatValue(companyGraphId))
                    .AND()
                    .WHERE("shareholder_name_id = ", SqlUtils.formatValue(humanGraphId))
                    .toString();
            ids.addAll(trigger.queryForList(sql, rs -> rs.getString(1)));
            return ids;
        }
    }

    @RequiredArgsConstructor
    private static final class EquityDirectSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate boss;
        private JdbcTemplate source;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            boss = new JdbcTemplate("157.prism_boss");
            source = new JdbcTemplate(QUERY_RDS);
            sink = new JdbcTemplate(SINK_RDS);
            sink.enableCache();
        }

        @Override
        public void invoke(String id, Context context) {
            String querySql = new SQL().SELECT("*")
                    .FROM(QUERY_TABLE)
                    .WHERE("id = " + SqlUtils.formatValue(id))
                    .toString();
            List<Map<String, Object>> columnMaps = source.queryForColumnMaps(querySql);
            if (columnMaps.isEmpty()) {
                String deleteSql = new SQL().DELETE_FROM(SINK_TABLE)
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                sink.update(deleteSql);
            } else {
                Map<String, Object> columnMap = columnMaps.get(0);
                String companyId = (String) columnMap.get("company_id");
                String companyName = (String) columnMap.get("company_name");
                String shareholderNameId = (String) columnMap.get("shareholder_name_id");
                String shareholderName = (String) columnMap.get("shareholder_name");
                String shareholderType = (String) columnMap.get("shareholder_type");
                String shareholderTypeShow = (String) columnMap.get("shareholder_type_show");
                String subscribedCapital = new BigDecimal(StrUtil.nullToDefault((String) columnMap.get("subscribed_capital"), "0"))
                        .setScale(12, RoundingMode.DOWN)
                        .toPlainString();
                String investmentRatio = new BigDecimal(StrUtil.nullToDefault((String) columnMap.get("investment_ratio"), "0"))
                        .setScale(12, RoundingMode.DOWN)
                        .toPlainString();
                String dataSource = (String) columnMap.get("data_source");
                String investmentStartTime = (String) columnMap.get("investment_start_time");
                Map<String, Object> resultMap = new HashMap<>();
                String pid;
                if ("1".equals(shareholderType)) {
                    String queryPidSql = new SQL().SELECT("human_pid")
                            .FROM("company_human_relation")
                            .WHERE("human_graph_id = " + SqlUtils.formatValue(shareholderNameId))
                            .WHERE("company_graph_id = " + SqlUtils.formatValue(companyId))
                            .WHERE("deleted = 0")
                            .toString();
                    String queryRes = boss.queryForObject(queryPidSql, rs -> rs.getString(1));
                    pid = StrUtil.blankToDefault(queryRes, shareholderNameId);
                } else {
                    pid = shareholderNameId;
                }
                // old
                resultMap.put("id", id);
                resultMap.put("company_id_invested", companyId);
                resultMap.put("tyc_unique_entity_id_invested", companyId);
                resultMap.put("tyc_unique_entity_name_invested", companyName);
                resultMap.put("investor_identity_type", shareholderType);
                resultMap.put("company_id_investor", shareholderNameId);
                resultMap.put("tyc_unique_entity_id_investor", pid);
                resultMap.put("tyc_unique_entity_name_investor", shareholderName);
                resultMap.put("equity_amount", subscribedCapital);
                resultMap.put("equity_amount_currency", "人民币");
                resultMap.put("equity_ratio", investmentRatio);
                resultMap.put("equity_relation_validation_year", 2024);
                resultMap.put("reference_pt_year", 2024);
                // new
                resultMap.put("company_id", companyId);
                resultMap.put("company_name", companyName);
                resultMap.put("shareholder_type", shareholderType);
                resultMap.put("shareholder_type_show", shareholderTypeShow);
                resultMap.put("shareholder_name_id", shareholderNameId);
                resultMap.put("shareholder_id", pid);
                resultMap.put("shareholder_name", shareholderName);
                resultMap.put("data_source", dataSource);
                resultMap.put("investment_start_time", investmentStartTime);
                // insert
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
                String insertSql = new SQL().INSERT_INTO(SINK_TABLE)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString() + ON_DUPLICATE_KEY_UPDATE;
                sink.update(insertSql);
            }
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
