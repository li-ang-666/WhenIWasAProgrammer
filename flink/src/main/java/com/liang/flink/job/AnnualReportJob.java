package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.annual.report.impl.*;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateImpl;
import com.liang.flink.service.data.update.DataUpdateService;
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

@Slf4j
@LocalConfigFile("annual-report.yml")
public class AnnualReportJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .addSink(new AnnualReportSink(config)).name("AnnualReportSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("AnnualReportJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    @DataUpdateImpl({
            ReportEquityChangeInfo.class,
            ReportShareholder.class,
            ReportOutboundInvestment.class,
            ReportWebinfo.class,
            Enterprise.class
    })
    private final static class AnnualReportSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate jdbcTemplate;
        private DataUpdateService<String> service;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("test");
            jdbcTemplate.enableCache();
            DataUpdateContext<String> context = new DataUpdateContext<>(AnnualReportSink.class);
            service = new DataUpdateService<>(context);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            List<String> sqls = service.invoke(singleCanalBinlog);
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
