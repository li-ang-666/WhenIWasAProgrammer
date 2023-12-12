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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

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
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = String.valueOf(columnMap.get("id"));
            if (id.equals("2476911709")) {
                Object bef = beforeColumnMap.get("social_security_staff_num");
                Object after = columnMap.get("social_security_staff_num");
                log.info("id: {}, bef: {}, aft: {}", id, bef, after);
            }
        }
    }
}
