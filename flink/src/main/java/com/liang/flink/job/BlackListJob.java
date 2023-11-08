package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.black.list.DorisDwdAppActive;
import com.liang.flink.project.black.list.DorisDwdOrderInfo;
import com.liang.flink.project.black.list.DorisDwdUserRegisterDetails;
import com.liang.flink.project.black.list.RatioPathCompany;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.TimeUnit;

@LocalConfigFile("black-list.yml")
public class BlackListJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> dataStream = StreamFactory.create(env);
        dataStream.addSink(new DummySink(config)).name("Dummy").setParallelism(config.getFlinkConfig().getOtherParallel());
        DaemonExecutor.launch("BlackList", new BlackList());
        env.execute("BlackListJob");
    }

    @RequiredArgsConstructor
    private final static class DummySink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
        }
    }

    @Slf4j
    private final static class BlackList implements Runnable {
        private final DorisTemplate dorisTemplate = new DorisTemplate("dorisSink");
        private final JdbcTemplate prismShareholderPath457 = new JdbcTemplate("457.prism_shareholder_path");

        @Override
        @SneakyThrows
        public void run() {
            while (true) {
                dorisTemplate.update(DorisDwdAppActive.get());
                dorisTemplate.update(DorisDwdUserRegisterDetails.get());
                dorisTemplate.update(DorisDwdOrderInfo.get());
                prismShareholderPath457.update(RatioPathCompany.get());
                TimeUnit.SECONDS.sleep(30);
            }
        }
    }
}
