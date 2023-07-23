package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.tyc.RatioPathCompanyTrigger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@LocalConfigFile("ratio-path-company.yml")
public class RatioPathCompanyJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .keyBy(new Distributor().with("investment_relation", e -> String.valueOf(e.getColumnMap().get("company_id_invested"))))
                .addSink(new RatioPathCompanySink(config)).name("RatioPathCompanySink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RatioPathCompanyJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class RatioPathCompanySink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final static long INTERVAL = 1000 * 60L;
        private final static long SIZE = 128L;
        private final static long MAX_SIZE = 1024L;
        private final Set<Long> companyIds = new HashSet<>();
        private final Config config;
        private RatioPathCompanyTrigger ratioPathCompanyTrigger;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            ratioPathCompanyTrigger = new RatioPathCompanyTrigger();
            new Thread(new Runnable() {
                private long lastSendTime;

                @Override
                public void run() {
                    while (true) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastSendTime >= INTERVAL || companyIds.size() >= SIZE) {
                            synchronized (companyIds) {
                                log.info("window trigger, currentTime: {}, lastTime: {}, size: {}",
                                        DateTimeUtils.fromUnixTime(currentTime / 1000, "yyyy-MM-dd HH:mm:ss"),
                                        DateTimeUtils.fromUnixTime(lastSendTime / 1000, "yyyy-MM-dd HH:mm:ss"),
                                        companyIds.size());
                                ratioPathCompanyTrigger.trigger(companyIds);
                                lastSendTime = currentTime;
                                companyIds.clear();
                            }
                        }
                    }
                }
            }).start();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) throws Exception {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String companyIdString = String.valueOf(columnMap.get("company_id_invested"));
            if (!StringUtils.isNumeric(companyIdString)) {
                return;
            }
            Long companyId = Long.parseLong(companyIdString);
            if (companyIds.contains(companyId)) {
                return;
            }
            while (true) {
                if (companyIds.size() <= MAX_SIZE) {
                    break;
                }
            }
            synchronized (companyIds) {
                companyIds.add(companyId);
            }
        }

        /**
         * 不背压的时候,改一改kafka的位点,随便重启
         * 背压的时候,需要从checkpoint恢复,保证每次checkpoint的时候,自定义的缓冲区里没有数据
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            synchronized (companyIds) {
                ratioPathCompanyTrigger.trigger(companyIds);
                companyIds.clear();
            }
        }
    }
}
