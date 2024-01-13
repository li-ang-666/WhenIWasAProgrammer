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
                    "create_time = nvl(create_time, now())",
                    "update_time = nvl(updatetime, now())"
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
