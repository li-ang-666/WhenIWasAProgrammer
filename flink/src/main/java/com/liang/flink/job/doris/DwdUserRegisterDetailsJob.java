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
 * DROP TABLE `test_db`.`dwd_user_register_details`;
 * CREATE TABLE `test_db`.`dwd_user_register_details` (
 * `tyc_user_id` bigint(20) NOT NULL DEFAULT "0" COMMENT '天眼查用户ID',
 * `mobile` bigint(20) NOT NULL DEFAULT "0" COMMENT '手机号',
 * `register_time` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '注册时间',
 * `vip_from_time` datetime NULL COMMENT 'VIP开始时间',
 * `vip_to_time` datetime NULL COMMENT 'VIP结束日期',
 * `user_type` int(11) NOT NULL DEFAULT "0" COMMENT '用户类型 0:普通,1:vip,2:媒体用户,3:3个月vip,4:6个月vip,5:12个月以上vip,6:24个月vip,7:26个月以上vip,-1:删除,-2:黑名单',
 * `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入doris时间',
 * `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新doris时间'
 * ) ENGINE=OLAP
 * UNIQUE KEY(`tyc_user_id`)
 * COMMENT '用户注册明细表-实时'
 * DISTRIBUTED BY HASH(`tyc_user_id`) BUCKETS 6
 * PROPERTIES (
 * "replication_allocation" = "tag.location.default: 3",
 * "enable_unique_key_merge_on_write" = "true",
 * "light_schema_change" = "true"
 * );
 */
@LocalConfigFile("dwd-user-register-details.yml")
public class DwdUserRegisterDetailsJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new DwdUserRegisterDetailsSink(config))
                .name("DwdUserRegisterDetailsSink")
                .uid("DwdUserRegisterDetailsSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("DwdUserRegisterDetailsJob");
    }

    @RequiredArgsConstructor
    private final static class DwdUserRegisterDetailsSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DorisTemplate dorisSink;
        private DorisSchema schema;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisSink = new DorisTemplate("dorisSink");
            dorisSink.enableCache();
            List<String> derivedColumns = Arrays.asList(
                    "tyc_user_id = id",
                    "mobile = mobile",
                    "register_time = create_time",
                    "vip_from_time = vip_from_time",
                    "vip_to_time = vip_to_time",
                    "user_type = state",
                    "create_time = nvl(create_time, current_timestamp())",
                    "update_time = nvl(updatetime, current_timestamp())"
            );
            schema = DorisSchema.builder()
                    .database("test_db")
                    .tableName("dwd_user_register_details")
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
