package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.bdp.equity.impl.RatioPathCompany;
import com.liang.flink.project.bdp.equity.impl.TycEntityMainReference;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateImpl;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@LocalConfigFile("bdp-equity.yml")
public class BdpEquityJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> sourceStream = StreamFactory.create(env);
        sourceStream
                .keyBy(new KeySelector<SingleCanalBinlog, String>() {
                    @Override
                    public String getKey(SingleCanalBinlog singleCanalBinlog) throws Exception {
                        String tableName = singleCanalBinlog.getTable();
                        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
                        if ("ratio_path_company".equals(tableName)) {
                            return columnMap.get("company_id") + "-" + columnMap.get("shareholder_id");
                        } else if ("tyc_entity_main_reference".equals(tableName)) {
                            return String.valueOf(columnMap.get("tyc_unique_entity_id"));
                        }
                        return "";
                    }
                })
                .addSink(new Sink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("BdpEquityJob");
    }

    @Slf4j
    @DataUpdateImpl({
            RatioPathCompany.class,
            TycEntityMainReference.class
    })
    public static final class Sink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DataUpdateService<SQL> service;

        public Sink(Config config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            DataUpdateContext<SQL> dataUpdateContext = new DataUpdateContext<>(this.getClass());
            service = new DataUpdateService<>(dataUpdateContext);

            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            if (indexOfThisSubtask == 0) {
                // 黑名单
                new Thread(() -> {
                    while (true) {
                        log.info("执行黑名单...");
                        String sql = new SQL()
                                .UPDATE("ratio_path_company")
                                .SET("is_controller = 0")
                                .SET("is_controlling_shareholder = 0")
                                .WHERE("company_id = 2338203553")
                                .toString();
                        new JdbcTemplate("prismShareholderPath").update(sql);
                        try {
                            TimeUnit.SECONDS.sleep(30);
                        } catch (Exception ignore) {
                        }
                    }
                }).start();
            }
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            service.invoke(singleCanalBinlog);
        }
    }
}
