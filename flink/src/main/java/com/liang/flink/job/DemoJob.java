package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.DemoTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.UUID;

@Slf4j
@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new DemoSink(config)).name("DemoSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("DemoJob");
    }

    @RequiredArgsConstructor
    private final static class DemoSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DemoTemplate demoTemplate;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            demoTemplate = new DemoTemplate();
            demoTemplate.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            // 1KB/条
            demoTemplate.update(UUID.randomUUID() + StringUtils.repeat(" ", 455));
        }
    }
}
