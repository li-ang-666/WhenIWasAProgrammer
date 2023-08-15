package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.investment.relation.InvestmentRelationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@LocalConfigFile("investment-relation.yml")
public class InvestmentRelationJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        Distributor distributor = new Distributor()
                .with("company_equity_relation_details", e -> String.valueOf(e.getColumnMap().get("company_id_invested")))
                .with("company_index", e -> String.valueOf(e.getColumnMap().get("company_id")))
                .with("enterprise", e -> String.valueOf(e.getColumnMap().get("graph_id")))
                .with("company_legal_person", e -> String.valueOf(e.getColumnMap().get("company_id")))
                .with("stock_actual_controller", e -> String.valueOf(e.getColumnMap().get("graph_id")))
                .with("personnel_employment_history", e -> String.valueOf(e.getColumnMap().get("company_id")));
        stream.keyBy(distributor)
                .addSink(new InvestmentRelationSink(config, distributor)).name("InvestmentRelationSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("InvestmentRelationJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class InvestmentRelationSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private final Distributor distributor;
        private InvestmentRelationService service;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new InvestmentRelationService();
            jdbcTemplate = new JdbcTemplate("457.prism_shareholder_path");
            jdbcTemplate.enableCache(5000, 1024);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            String key = distributor.getKey(singleCanalBinlog);
            List<SQL> sqls = service.invoke(key);
            jdbcTemplate.update(sqls.stream().map(String::valueOf).collect(Collectors.toList()));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            jdbcTemplate.flush();
        }

        @Override
        public void finish() {
            jdbcTemplate.flush();
        }

        @Override
        public void close() {
            jdbcTemplate.flush();
            ConfigUtils.unloadAll();
        }
    }
}
