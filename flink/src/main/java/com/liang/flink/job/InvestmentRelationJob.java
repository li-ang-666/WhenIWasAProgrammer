package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.AbstractSQL;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
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
import java.util.stream.Collectors;

@Slf4j
@LocalConfigFile("investment-relation.yml")
public class InvestmentRelationJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new InvestmentRelationSink(config)).name("InvestmentRelationSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("InvestmentRelationJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    @DataUpdateImpl({

    })
    private final static class InvestmentRelationSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private DataUpdateService<SQL> service;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            DataUpdateContext<SQL> context = new DataUpdateContext<>(InvestmentRelationSink.class);
            service = new DataUpdateService<>(context);
            jdbcTemplate = new JdbcTemplate("prismShareholderPath");
            jdbcTemplate.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) throws Exception {
            List<SQL> result = service.invoke(singleCanalBinlog);
            List<String> sqls = result.stream().map(AbstractSQL::toString).collect(Collectors.toList());
            jdbcTemplate.update(sqls);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            jdbcTemplate.flush();
        }

        @Override
        public void finish() throws Exception {
            jdbcTemplate.flush();
        }

        @Override
        public void close() throws Exception {
            jdbcTemplate.flush();
            ConfigUtils.unloadAll();
        }
    }
}
