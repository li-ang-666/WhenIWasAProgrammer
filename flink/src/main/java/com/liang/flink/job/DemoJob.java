package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.filesystem.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
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
        StreamFactory.create(env)
                .addSink(new DemoSink(ConfigUtils.getConfig())).setParallelism(1);
        env.execute("DemoJob");
    }

    @RequiredArgsConstructor
    private final static class DemoSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private ObsWriter obsWriter;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            obsWriter = new ObsWriter("obs://hadoop-obs/flink/enterprise");
            obsWriter.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog value, Context context) throws Exception {
            System.out.println((JsonUtils.toString(value)));
        }
    }
}
