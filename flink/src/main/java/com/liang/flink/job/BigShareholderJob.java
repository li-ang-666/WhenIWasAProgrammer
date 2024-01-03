package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@LocalConfigFile("big-shareholder.yml")
public class BigShareholderJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new BigShareholderSink(config))
                .name("BigShareholderSink")
                .uid("BigShareholderSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("BigShareholderJob");
    }

    @RequiredArgsConstructor
    private final static class BigShareholderSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private static volatile Set<String> Listed_company = null;
        private final Config config;
        private JdbcTemplate jdbcTemplate457;
        private JdbcTemplate jdbcTemplate463;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate457 = new JdbcTemplate("457.prism_shareholder_path");
            jdbcTemplate457.enableCache();
            jdbcTemplate463 = new JdbcTemplate("463.bdp_equity");
            jdbcTemplate463.enableCache();
            if (Listed_company == null) {
                synchronized (BigShareholderSink.class) {
                    if (Listed_company == null) {
                        JdbcTemplate jdbcTemplate157 = new JdbcTemplate("157.listed_base");
                        String query = new SQL().SELECT_DISTINCT("company_id")
                                .FROM("company_bond_plates")
                                .WHERE("company_id is not null and company_id > 0")
                                .WHERE("listed_status is not null and listed_status not in (0, 3, 5, 8, 9)")
                                .toString();
                        Listed_company = new HashSet<>(jdbcTemplate157.queryForList(query, rs -> rs.getString(1)));
                    }
                }
            }
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            String table = singleCanalBinlog.getTable();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            if (table.equals("ratio_path_company")) {
                String companyId = String.valueOf(columnMap.get("company_id"));
                String isBigShareholder = String.valueOf(columnMap.get("is_big_shareholder"));
                if ("1".equals(isBigShareholder) && !Listed_company.contains(companyId)) {
                    String update = new SQL().UPDATE("ratio_path_company")
                            .SET("is_big_shareholder = 0")
                            .WHERE("id = " + columnMap.get("id"))
                            .toString();
                    jdbcTemplate457.update(update);
                }
            } else if (table.equals("shareholder_identity_type_details")) {
                String companyId = String.valueOf(columnMap.get("tyc_unique_entity_id"));
                String shareholderIdentityType = String.valueOf(columnMap.get("shareholder_identity_type"));
                if ("1".equals(shareholderIdentityType) && !Listed_company.contains(companyId)) {
                    String delete = new SQL().DELETE_FROM("shareholder_identity_type_details")
                            .WHERE("id = " + columnMap.get("id"))
                            .toString();
                    jdbcTemplate463.update(delete);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            jdbcTemplate457.flush();
            jdbcTemplate463.flush();
        }

        @Override
        public void finish() {
            jdbcTemplate457.flush();
            jdbcTemplate463.flush();
        }

        @Override
        public void close() {
            jdbcTemplate457.flush();
            jdbcTemplate463.flush();
        }
    }
}
