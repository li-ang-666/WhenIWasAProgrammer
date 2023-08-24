package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.company.base.info.CompanyBaseInfoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@LocalConfigFile("company-base-info.yml")
public class CompanyBaseInfoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        Distributor distributor = new Distributor()
                .with("tyc_entity_general_property_reference", e -> String.valueOf(e.getColumnMap().get("company_cid")))
                .with("enterprise", e -> String.valueOf(e.getColumnMap().get("id")))
                .with("company", e -> String.valueOf(e.getColumnMap().get("id")))
                .with("company_clean_info", e -> String.valueOf(e.getColumnMap().get("id")))
                .with("gov_unit", e -> String.valueOf(e.getColumnMap().get("company_id")));
        stream
                .rebalance()
                .map(new CompanyBaseInfoMap(config)).name("CompanyBaseInfoMap").setParallelism(config.getFlinkConfig().getOtherParallel())
                .keyBy(distributor)
                .addSink(new CompanyBaseInfoSink(config, distributor)).name("CompanyBaseInfoSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("CompanyBaseInfoJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class CompanyBaseInfoMap extends RichMapFunction<SingleCanalBinlog, SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;


        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("464.prism");
        }

        @Override
        public SingleCanalBinlog map(SingleCanalBinlog singleCanalBinlog) {
            if (singleCanalBinlog.getTable().equals("tyc_entity_general_property_reference")) {
                Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
                Object tycUniqueEntityId = columnMap.get("tyc_unique_entity_id");
                String sql = new SQL().SELECT("id")
                        .FROM("enterprise")
                        .WHERE("deleted = 0")
                        .WHERE("graph_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                        .toString();
                String res = jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
                columnMap.put("company_cid", res != null ? res : "0");
            }
            return singleCanalBinlog;
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class CompanyBaseInfoSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Set<String> companyCids = ConcurrentHashMap.newKeySet();
        private final Config config;
        private final Distributor distributor;
        private CompanyBaseInfoService service;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new CompanyBaseInfoService();
            jdbcTemplate = new JdbcTemplate("427.test");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            String cid = String.valueOf(distributor.getKey(singleCanalBinlog));
            synchronized (companyCids) {
                companyCids.add(cid);
            }
            if (companyCids.size() >= 10240) {
                flush();
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
            ConfigUtils.unloadAll();
        }

        private void flush() {
            synchronized (companyCids) {
                ArrayList<String> buffer = new ArrayList<>();
                for (String companyCid : companyCids) {
                    List<String> sqls = service.invoke(companyCid);
                    buffer.addAll(sqls);
                    if (buffer.size() >= 1024) {
                        jdbcTemplate.update(buffer);
                        buffer.clear();
                    }
                }
                jdbcTemplate.update(buffer);
                buffer.clear();
            }
        }
    }
}
