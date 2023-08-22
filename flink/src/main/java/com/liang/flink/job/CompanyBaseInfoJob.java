package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@LocalConfigFile("company-base-info.yml")
public class CompanyBaseInfoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .addSink(new CompanyBaseInfoSink(config)).name("CompanyBaseInfoSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("CompanyBaseInfoJob");
    }

    @RequiredArgsConstructor
    private final static class CompanyBaseInfoSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {

        }

        @Override
        public void invoke(SingleCanalBinlog value, Context context) {

        }

        @Override
        public void close() {
            ConfigUtils.unloadAll();
        }
    }
}
