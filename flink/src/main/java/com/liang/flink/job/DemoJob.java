package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@Slf4j
@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env).rebalance().addSink(new DemoSink(config)).name("DemoSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("DemoJob");
    }

    @RequiredArgsConstructor
    private final static class DemoSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("427.test");
            jdbcTemplate.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
        }
    }
}
