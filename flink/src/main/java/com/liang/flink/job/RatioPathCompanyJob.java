package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.ratio.path.company.RatioPathCompanyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@LocalConfigFile("ratio-path-company.yml")
public class RatioPathCompanyJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .flatMap(new RatioPathCompanyFlatMap(config)).setParallelism(config.getFlinkConfig().getOtherParallel())
                .keyBy(e -> e)
                .addSink(new RatioPathCompanySink(config)).name("RatioPathCompanySink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RatioPathCompanyJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class RatioPathCompanyFlatMap extends RichFlatMapFunction<SingleCanalBinlog, Long> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("457.prism_shareholder_path");
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<Long> out) {
            Set<Long> result = new HashSet<>();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String companyIdString = String.valueOf(columnMap.get("company_id_invested"));
            if (StringUtils.isNumeric(companyIdString)) {
                result.add(Long.parseLong(companyIdString));
            }
            String companyEntityInlink = String.valueOf(columnMap.get("company_entity_inlink"));
            String[] split = companyEntityInlink.split(":");
            String shareholderIdString = split[split.length - 2];
            String sql = new SQL()
                    .SELECT("distinct company_id")
                    .FROM("ratio_path_company")
                    .WHERE(String.format("shareholder_id in ('%s','%s')", companyIdString, shareholderIdString))
                    .toString();
            jdbcTemplate.queryForList(sql, rs -> {
                String companyId = rs.getString(1);
                if (StringUtils.isNumeric(companyId)) {
                    result.add(Long.parseLong(companyId));
                }
                return null;
            });
            result.forEach(out::collect);
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class RatioPathCompanySink extends RichSinkFunction<Long> implements CheckpointedFunction {
        private final static int INTERVAL = 1000 * 30;
        private final static int SIZE = 8;
        private final Set<Long> companyIds = ConcurrentHashMap.newKeySet();
        private final Config config;
        private RatioPathCompanyService service;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new RatioPathCompanyService();
            DaemonExecutor.launch("RatioPathCompanyServiceThread", new Runnable() {
                private long lastSendTime = System.currentTimeMillis();

                @Override
                public void run() {
                    while (true) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastSendTime >= INTERVAL || companyIds.size() >= SIZE) {
                            synchronized (companyIds) {
                                String lastSendTimeString = DateTimeUtils.fromUnixTime(lastSendTime / 1000);
                                if (!companyIds.isEmpty()) {
                                    log.info("window trigger, lastTime: {}, size: {}, companyIds: {}", lastSendTimeString, companyIds.size(), companyIds);
                                } else {
                                    log.info("window trigger, lastTime: {}, empty", lastSendTimeString);
                                }
                                service.invoke(companyIds);
                                companyIds.clear();
                            }
                            lastSendTime = currentTime;
                        }
                    }
                }
            });
        }

        @Override
        public void invoke(Long companyId, Context context) {
            synchronized (companyIds) {
                companyIds.add(companyId);
            }
        }

        /**
         * 不背压的时候,改一改kafka的位点,随便重启
         * 背压的时候,需要从checkpoint恢复,保证每次checkpoint的时候,自定义的缓冲区里没有数据
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush("snapshotState");
        }

        @Override
        public void finish() {
            flush("finish");
        }

        @Override
        public void close() {
            flush("close");
            ConfigUtils.unloadAll();
        }

        private void flush(String method) {
            synchronized (companyIds) {
                if (!companyIds.isEmpty()) {
                    log.info("{}, size: {}, companyIds: {}", method, companyIds.size(), companyIds);
                } else {
                    log.info("{}, empty", method);
                }
                service.invoke(companyIds);
                companyIds.clear();
            }
        }
    }
}
