package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.company.base.info.CompanyBaseInfoService;
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

@LocalConfigFile("company-base-info.yml")
public class CompanyBaseInfoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        Distributor distributor = new Distributor()
                .with("enterprise", e -> String.valueOf(e.getColumnMap().get("id")))
                .with("gov_unit", e -> String.valueOf(e.getColumnMap().get("company_id")));
        stream
                .keyBy(distributor)
                .addSink(new CompanyBaseInfoSink(config, distributor)).name("CompanyBaseInfoSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("CompanyBaseInfoJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class CompanyBaseInfoSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
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
            List<String> sqls = service.invoke(cid);
            jdbcTemplate.update(sqls);
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
