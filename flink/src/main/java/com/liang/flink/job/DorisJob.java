package com.liang.flink.job;

import com.alibaba.google.common.util.concurrent.RateLimiter;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

/**
 * dwd_user_register_details
 * dwd_coupon_info
 * dwd_app_active
 */
@Slf4j
@LocalConfigFile("doris/dwd_app_active.yml")
public class DorisJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new DorisSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("DorisSink")
                .uid("DorisSink");
        env.execute("doris." + config.getDorisSchema().getTableName());
    }

    @RequiredArgsConstructor
    private final static class DorisSink extends RichSinkFunction<SingleCanalBinlog> {
        private static final RateLimiter RATE_LIMITER = RateLimiter.create(6000);
        private final Config config;
        private DorisWriter dorisWriter;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisWriter = new DorisWriter("dorisSink", 1024 * 1024 * 1024);
            config.getDorisSchema().setUniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON);
            log.info("doris schema: {}", config.getDorisSchema());
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            RATE_LIMITER.acquire();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            columnMap.put(DorisSchema.DEFAULT_UNIQUE_DELETE_COLUMN, singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE ? 1 : 0);
            dorisWriter.write(new DorisOneRow(config.getDorisSchema(), columnMap));
        }
    }
}
