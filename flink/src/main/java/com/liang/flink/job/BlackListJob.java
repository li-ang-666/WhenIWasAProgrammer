package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
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
        @Override
        @SneakyThrows
        public void run() {
            while (true) {
                log.info("run");
                new JdbcTemplate("116.prism").update("delete from equity_ratio where company_graph_id = 3450849811");
                TimeUnit.SECONDS.sleep(30);
            }
        }
    }
}
