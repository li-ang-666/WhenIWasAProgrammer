package com.liang.flink.job;

import com.alibaba.google.common.util.concurrent.RateLimiter;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Slf4j
@LocalConfigFile("doris/dwd_coupon_info.yml")
public class DorisJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.flatMap(new DorisFlatMap(config))
                .setParallelism(1)
                .name("DorisFlatMap")
                .uid("DorisFlatMap")
                .rebalance()
                .addSink(new DorisSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("DorisSink")
                .uid("DorisSink");
        env.execute(config.getDorisSchema().getDatabase() + "." + config.getDorisSchema().getDatabase());
    }

    @RequiredArgsConstructor
    private final static class DorisSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DorisTemplate dorisSink;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisSink = new DorisTemplate("dorisSink");
            dorisSink.enableCache(1000, 128);
            config.getDorisSchema().setUniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON);
            log.info("doris schema: {}", config.getDorisSchema());
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            columnMap.put("__DORIS_DELETE_SIGN__", singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE);
            dorisSink.update(new DorisOneRow(config.getDorisSchema(), columnMap));
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class DorisFlatMap extends RichFlatMapFunction<SingleCanalBinlog, SingleCanalBinlog> {
        private static final RateLimiter RATE_LIMITER = RateLimiter.create(6000);
        private final Config config;
        private String sinkTable;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sinkTable = config.getDorisSchema().getTableName();
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<SingleCanalBinlog> out) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            if ("dwd_app_active".equals(sinkTable)) {
                String createTime = String.valueOf(columnMap.get("create_time"));
                String appId2 = String.valueOf(columnMap.get("app_id2"));
                String minPt = LocalDateTime.now().plusDays(-29).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                if (TycUtils.isValidName(appId2) && TycUtils.isDateTime(createTime) && createTime.compareTo(minPt) > 0) {
                    RATE_LIMITER.acquire();
                    out.collect(singleCanalBinlog);
                }
            } else if ("dwd_coupon_info".equals(sinkTable)) {
                String bizCode = String.valueOf(columnMap.get("biz_code"));
                String deletedStatus = String.valueOf(columnMap.get("deleted_status"));
                String promotionCode = String.valueOf(columnMap.get("promotion_code"));
                if ("1".equals(bizCode) && "0".equals(deletedStatus) && TycUtils.isValidName(promotionCode)) {
                    RATE_LIMITER.acquire();
                    out.collect(singleCanalBinlog);
                }
            } else {
                RATE_LIMITER.acquire();
                out.collect(singleCanalBinlog);
            }
        }
    }
}
