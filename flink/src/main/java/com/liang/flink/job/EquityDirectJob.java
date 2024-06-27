package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
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
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@LocalConfigFile("equity-direct.yml")
public class EquityDirectJob {
    private static final String QUERY_RDS_BASE = "435.company_base";
    private static final String QUERY_TABLE_BASE = "shareholder_investment_relation_info";

    private static final String QUERY_RDS_HK = "041.listed_base";
    private static final String QUERY_TABLE_HK = "main_shareholder_hk";

    private static final String QUERY_RDS_JUDGE = "142.company_base";
    private static final String QUERY_TABLE_JUDGE = "show_shareholder_tab";

    private static final String QUERY_RDS_RELATION = "157.prism_boss";
    private static final String QUERY_TABLE_RELATION = "company_human_relation";

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

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            String table = singleCanalBinlog.getTable();
            if (table.equals(QUERY_TABLE_RELATION)) {
                out.collect((String) singleCanalBinlog.getBeforeColumnMap().get("company_graph_id"));
                out.collect((String) singleCanalBinlog.getAfterColumnMap().get("company_graph_id"));
            } else {
                out.collect((String) singleCanalBinlog.getColumnMap().get("company_id"));
            }
        }
    }

    @RequiredArgsConstructor
    private static final class EquityDirectSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Roaring64Bitmap bitmap = new Roaring64Bitmap();
        private final Config config;
        private JdbcTemplate rdsBase;
        private JdbcTemplate rdsHk;
        private JdbcTemplate rdsJudge;
        private JdbcTemplate rdsRelation;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            rdsBase = new JdbcTemplate(QUERY_RDS_BASE);
            rdsHk = new JdbcTemplate(QUERY_RDS_HK);
            rdsJudge = new JdbcTemplate(QUERY_RDS_JUDGE);
            rdsRelation = new JdbcTemplate(QUERY_RDS_RELATION);
            sink = new JdbcTemplate(SINK_RDS);
            sink.enableCache();
        }

        @Override
        public void invoke(String companyId, Context context) {
            synchronized (bitmap) {
                if (TycUtils.isUnsignedId(companyId)) {
                    bitmap.add(Long.parseLong(companyId));
                }
                // 全量修复的时候, 来一条计算一条
                if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.Repair) {
                    flush();
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
            sink.flush();
        }

        @Override
        public void finish() {
            flush();
            sink.flush();
        }

        @Override
        public void close() {
            flush();
            sink.flush();
        }

        private void flush() {
            synchronized (bitmap) {
                bitmap.forEach(this::consumeCompanyId);
                bitmap.clear();
            }
        }

        private void consumeCompanyId(long companyId) {
            delete(companyId);
            boolean isHk = judgeIsHk(companyId);
            List<Map<String, Object>> columnMaps = queryColumnMaps(companyId, isHk);
            for (Map<String, Object> columnMap : columnMaps) {
                Map<String, Object> resultMap = parseColumnMap(columnMap, isHk);
                insertColumnMap(resultMap);
            }
        }

        private void delete(Object companyId) {
            String sql = new SQL().DELETE_FROM(SINK_TABLE)
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            sink.update(sql);
        }

        private boolean judgeIsHk(Object companyId) {
            String sql = new SQL().SELECT("1")
                    .FROM(QUERY_TABLE_JUDGE)
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .WHERE("search_button_show in (2, 3)")
                    .WHERE("latest_public_data_source = 4")
                    .toString();
            return rdsJudge.queryForObject(sql, rs -> rs.getString(1)) != null;
        }

        private List<Map<String, Object>> queryColumnMaps(Object companyId, boolean isHk) {
            String sql;
            if (isHk) {
                String innerSql = new SQL().SELECT("max(main_shareholder_announcement_date)")
                        .FROM(QUERY_TABLE_HK)
                        .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                        .WHERE("is_deleted = 0")
                        .LIMIT(1)
                        .toString();
                sql = new SQL().SELECT("*")
                        .FROM(QUERY_TABLE_HK)
                        .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                        .WHERE("is_deleted = 0")
                        .WHERE("main_shareholder_announcement_date = (" + innerSql + ")")
                        .toString();
            } else {
                sql = new SQL().SELECT("*")
                        .FROM(QUERY_TABLE_BASE)
                        .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                        .toString();
            }
            JdbcTemplate query = isHk ? rdsHk : rdsBase;
            return query.queryForColumnMaps(sql);
        }

        private Map<String, Object> parseColumnMap(Map<String, Object> columnMap, boolean isHk) {
            Map<String, Object> resultMap = new HashMap<>();
            String companyId;
            String companyName;
            String shareholderNameId;
            String shareholderName;
            String shareholderType;
            String shareholderTypeShow;
            String subscribedCapital;
            String investmentRatio;
            String pid;
            String shareType;
            companyId = (String) columnMap.get("company_id");
            shareholderTypeShow = (String) columnMap.get("shareholder_type_show");
            if (isHk) {
                companyName = queryCompanyName(companyId);
                shareholderNameId = (String) columnMap.get("main_shareholder_gid");
                shareholderName = (String) columnMap.get("main_shareholder_name");
                switch ((String) columnMap.get("is_main_shareholder_org")) {
                    case "0":
                        shareholderType = "1";
                        break;
                    case "1":
                        shareholderType = "2";
                        break;
                    default:
                        shareholderType = "3";
                        break;
                }
                subscribedCapital = formatNumber((String) columnMap.get("hk_shares_cnt_total_holding"), false);
                investmentRatio = formatNumber((String) columnMap.get("hk_shares_ratio_per_total_issue_shares_cnt"), true);
                shareType = "股";
            } else {
                companyName = (String) columnMap.get("company_name");
                shareholderNameId = (String) columnMap.get("shareholder_name_id");
                shareholderName = (String) columnMap.get("shareholder_name");
                shareholderType = (String) columnMap.get("shareholder_type");
                subscribedCapital = formatNumber((String) columnMap.get("subscribed_capital"), false);
                investmentRatio = formatNumber((String) columnMap.get("investment_ratio"), false);
                shareType = ((String) columnMap.get("share_type")).contains("股") ? "万股" : "人民币";
            }
            pid = "1".equals(shareholderType) ? queryPid(shareholderNameId, companyId) : shareholderNameId;
            // old
            resultMap.put("company_id_invested", companyId);
            resultMap.put("tyc_unique_entity_id_invested", companyId);
            resultMap.put("tyc_unique_entity_name_invested", companyName);
            resultMap.put("investor_identity_type", shareholderType);
            resultMap.put("company_id_investor", shareholderNameId);
            resultMap.put("tyc_unique_entity_id_investor", pid);
            resultMap.put("tyc_unique_entity_name_investor", shareholderName);
            resultMap.put("equity_amount", subscribedCapital);
            resultMap.put("equity_amount_currency", shareType);
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
            return resultMap;
        }

        private String queryCompanyName(Object companyId) {
            String sql = new SQL().SELECT("company_name")
                    .FROM("company_index")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            return StrUtil.blankToDefault(rdsBase.queryForObject(sql, rs -> rs.getString(1)), "");
        }

        private String queryPid(String shareholderNameId, String companyId) {
            String queryPidSql = new SQL().SELECT("human_pid")
                    .FROM(QUERY_TABLE_RELATION)
                    .WHERE("human_graph_id = " + SqlUtils.formatValue(shareholderNameId))
                    .WHERE("company_graph_id = " + SqlUtils.formatValue(companyId))
                    .WHERE("deleted = 0")
                    .toString();
            String queryRes = rdsRelation.queryForObject(queryPidSql, rs -> rs.getString(1));
            return StrUtil.blankToDefault(queryRes, shareholderNameId);
        }

        private String formatNumber(String number, boolean divide100) {
            return new BigDecimal(StrUtil.nullToDefault(number, "0"))
                    .abs()
                    .divide(divide100 ? new BigDecimal(100) : new BigDecimal(1), 12, RoundingMode.DOWN)
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
        }

        private void insertColumnMap(Map<String, Object> columnMap) {
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String insertSql = new SQL().INSERT_INTO(SINK_TABLE)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(insertSql);
        }
    }
}
