package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.SQL;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.bdp.equity.black.BdpEquityCleaner;
import com.liang.flink.project.bdp.equity.impl.TycEntityMainReference;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateImpl;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

@Slf4j
@LocalConfigFile("bdp-equity.yml")
@DataUpdateImpl({
        TycEntityMainReference.class
})
public class BdpEquityJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> sourceStream = StreamFactory.create(env);
        sourceStream
                .keyBy(getDistributor())
                .addSink(new BdpEquitySink(config)).name("BdpEquitySink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("BdpEquityJob");
    }

    private static Distributor getDistributor() {
        return new Distributor()
                .with("tyc_entity_main_reference", e -> {
                    Map<String, Object> columnMap = e.getColumnMap();
                    return String.valueOf(columnMap.get("tyc_unique_entity_id"));
                });
    }

    @Slf4j
    public static final class BdpEquitySink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DataUpdateService<SQL> service;

        public BdpEquitySink(Config config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            DataUpdateContext<SQL> dataUpdateContext = new DataUpdateContext<>(BdpEquityJob.class);
            service = new DataUpdateService<>(dataUpdateContext);
            // 黑名单
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                DaemonExecutor.launch("BdpEquityCleaner", new BdpEquityCleaner());
            }
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            service.invoke(singleCanalBinlog);
        }
    }
}
