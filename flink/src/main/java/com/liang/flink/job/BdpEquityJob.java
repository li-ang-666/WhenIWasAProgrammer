package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
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

@Slf4j
public class BdpEquityJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"bdp-equity.yml"};
        }
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
    @DataUpdateImpl({RatioPathCompany.class, TycEntityMainReference.class})
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
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            service.invoke(singleCanalBinlog);
        }
    }
}
