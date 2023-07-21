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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
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
                .flatMap(new RatioPathCompanyFlatMap(config)).name("RatioPathCompanyFlatMap").setParallelism(config.getFlinkConfig().getOtherParallel())
                .addSink(new RatioPathCompanySink(config)).name("RatioPathCompanySink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RatioPathCompanyJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class RatioPathCompanyFlatMap extends RichFlatMapFunction<SingleCanalBinlog, Set<Long>> {
        private final static long INTERVAL = 1000 * 60L;
        private final static long SIZE = 1024;
        private final Set<Long> companyIds = new HashSet<>();
        private final Config config;
        private volatile long lastSendTime = 0;
        private Thread sendThread;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<Set<Long>> out) throws Exception {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String companyId = String.valueOf(columnMap.get("company_id_invested"));
            if (StringUtils.isNumeric(companyId)) {
                synchronized (companyIds) {
                    companyIds.add(Long.parseLong(companyId));
                }
            }
            if (sendThread != null) {
                return;
            }
            sendThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastSendTime >= INTERVAL || companyIds.size() >= SIZE) {
                            log.info("window trigger, currentTime: {}, lastTime: {}, size: {}",
                                    DateTimeUtils.fromUnixTime(currentTime / 1000, "yyyy-MM-dd HH:mm:ss"),
                                    DateTimeUtils.fromUnixTime(lastSendTime / 1000, "yyyy-MM-dd HH:mm:ss"),
                                    companyIds.size());
                            synchronized (companyIds) {
                                out.collect(new HashSet<>(companyIds));
                                lastSendTime = currentTime;
                                companyIds.clear();
                            }
                        }
                    }
                }
            });
            sendThread.start();
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class RatioPathCompanySink extends RichSinkFunction<Set<Long>> {
        private final Config config;
        private RatioPathCompanyTrigger ratioPathCompanyTrigger;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            ratioPathCompanyTrigger = new RatioPathCompanyTrigger();
        }

        @Override
        public void invoke(Set<Long> companyIds, Context context) throws Exception {
            try {
                ratioPathCompanyTrigger.trigger(companyIds);
            } catch (Exception e) {
                log.error("RatioPathCompanySink invoke({})", companyIds);
            }
        }
    }
}
