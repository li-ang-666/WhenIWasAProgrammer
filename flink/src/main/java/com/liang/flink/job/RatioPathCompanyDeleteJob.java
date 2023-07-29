package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.ratio.path.company.RatioPathCompanyDao;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@LocalConfigFile("ratio-path-company-delete.yml")
public class RatioPathCompanyDeleteJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .keyBy(new Distributor().with("ratio_path_company", e -> String.valueOf(e.getColumnMap().get("company_id"))))
                .addSink(new RatioPathCompanyDeleteSink(config)).name("RatioPathCompanyDeleteSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RatioPathCompanyDeleteJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class RatioPathCompanyDeleteSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Set<Long> companyIds = ConcurrentHashMap.newKeySet();
        private final Config config;
        private RatioPathCompanyDao dao;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dao = new RatioPathCompanyDao();
            jdbcTemplate = new JdbcTemplate("prismShareholderPath");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String companyIdString = String.valueOf(columnMap.get("company_id"));
            companyIds.add(Long.parseLong(companyIdString));
            if (companyIds.size() >= 1024) {
                synchronized (companyIds) {
                    for (Long companyId : companyIds) {
                        String res = jdbcTemplate.queryForObject("select 1 from investment_relation where company_id_invested = " + companyId, rs -> rs.getString(1));
                        if (res == null) {
                            dao.deleteAll(companyId);
                        }
                    }
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            synchronized (companyIds) {
                for (Long companyId : companyIds) {
                    String res = jdbcTemplate.queryForObject("select 1 from investment_relation where company_id_invested = " + companyId, rs -> rs.getString(1));
                    if (res == null) {
                        dao.deleteAll(companyId);
                    }
                }
            }
        }

        @Override
        public void finish() throws Exception {
            synchronized (companyIds) {
                for (Long companyId : companyIds) {
                    String res = jdbcTemplate.queryForObject("select 1 from investment_relation where company_id_invested = " + companyId, rs -> rs.getString(1));
                    if (res == null) {
                        dao.deleteAll(companyId);
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (companyIds) {
                for (Long companyId : companyIds) {
                    String res = jdbcTemplate.queryForObject("select 1 from investment_relation where company_id_invested = " + companyId, rs -> rs.getString(1));
                    if (res == null) {
                        dao.deleteAll(companyId);
                    }
                }
            }
        }
    }
}
