package com.liang.flink.job.doris;

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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@LocalConfigFile("dwd-coupon-info.yml")
public class DwdCouponInfoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .filter(e -> {
                    String bizCode = String.valueOf(e.getColumnMap().get("biz_code"));
                    String deletedStatus = String.valueOf(e.getColumnMap().get("deleted_status"));
                    String promotionCode = String.valueOf(e.getColumnMap().get("promotion_code"));
                    return "1".equals(bizCode) && "0".equals(deletedStatus) && TycUtils.isValidName(promotionCode);
                })
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
                    "promotion_code = nvl(promotion_code, '')",
                    "unique_user_id = nvl(user_id, 0)",
                    "promotion_id = nvl(promotion_id, 0)",
                    "use_status = nvl(use_status, 0)",
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
