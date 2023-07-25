package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SnowflakeUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;

@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        env
                .addSource(new DemoSource(config))
                .addSink(new DemoSink(config)).setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("DemoJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class DemoSource extends RichSourceFunction<Integer> {
        private final Config config;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (true) {
                for (int i = 1; i <= 1024 * 1024; i++) {
                    ctx.collect(i);
                }
            }
        }

        @Override
        public void cancel() {
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class DemoSink extends RichSinkFunction<Integer> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            SnowflakeUtils.init(DemoJob.class.getSimpleName());
            jdbcTemplate = new JdbcTemplate("test");
        }

        @Override
        public void invoke(Integer input, Context context) throws Exception {
            ArrayList<String> list = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                list.add(String.format("insert into id_test values(%s)", SnowflakeUtils.nextId()));
            }
            jdbcTemplate.update(list);
        }
    }
}
