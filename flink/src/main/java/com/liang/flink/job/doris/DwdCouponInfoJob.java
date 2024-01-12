package com.liang.flink.job.doris;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * DROP TABLE `test_db`.`dwd_coupon_info`;
 * CREATE TABLE `test_db`.`dwd_coupon_info` (
 * `promotion_code` varchar(65530) COMMENT '优惠码',
 * `unique_user_id` bigint COMMENT '天眼查用户ID-new',
 * `promotion_id` bigint COMMENT '优惠ID',
 * `use_status` int COMMENT '使用状态',
 * `receive_time` datetime COMMENT '发券时间',
 * `effective_time` datetime COMMENT '有效开始日期',
 * `expiration_time` datetime COMMENT '有效结束日期',
 * `create_time` datetime COMMENT '写入doris时间',
 * `update_time` datetime COMMENT '更新doris时间'
 * ) ENGINE=OLAP
 * UNIQUE KEY(`promotion_code`)
 * COMMENT '优惠券'
 * DISTRIBUTED BY HASH(`promotion_code`) BUCKETS 6
 * PROPERTIES (
 * "replication_allocation" = "tag.location.default: 3",
 * "enable_unique_key_merge_on_write" = "true",
 * "light_schema_change" = "true"
 * );
 */
@LocalConfigFile("dwd-coupon-info.yml")
public class DwdCouponInfoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new DwdCouponInfoSink(config))
                .name("DwdCouponInfoSink")
                .uid("DwdCouponInfoSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("DwdCouponInfoJob");
    }

    @RequiredArgsConstructor
    private final static class DwdCouponInfoSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DorisTemplate dorisSink;
        private DorisSchema schema;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisSink = new DorisTemplate("dorisSink");
            dorisSink.enableCache();
            List<String> derivedColumns = Arrays.asList(
                    "promotion_code = promotion_code",
                    "unique_user_id = user_id",
                    "promotion_id = promotion_id",
                    "use_status = use_status",
                    "receive_time = from_unixtime(receive_time/1000)",
                    "effective_time = from_unixtime(effective_time/1000)",
                    "expiration_time = from_unixtime(expiration_time/1000)",
                    "create_time = now()",
                    "update_time = now()"
            );
            schema = DorisSchema.builder()
                    .database("test_db")
                    .tableName("dwd_coupon_info")
                    .uniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON)
                    .derivedColumns(derivedColumns)
                    .build();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            columnMap.put("__DORIS_DELETE_SIGN__", singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE);
            dorisSink.update(new DorisOneRow(schema, columnMap));
        }
    }
}
