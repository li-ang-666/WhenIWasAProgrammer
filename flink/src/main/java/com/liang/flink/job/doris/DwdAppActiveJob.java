package com.liang.flink.job.doris;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateUtils;
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

@LocalConfigFile("dwd-app-active.yml")
public class DwdAppActiveJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .filter(e -> {
                    String appId2 = String.valueOf(e.getColumnMap().get("app_id2"));
                    String createTime = String.valueOf(e.getColumnMap().get("create_time"));
                    String ThirtyDaysAgo = DateUtils.getOfflinePt(29, "yyyy-MM-dd");
                    return TycUtils.isValidName(appId2)
                            && TycUtils.isDateTime(createTime)
                            && (createTime.compareTo(ThirtyDaysAgo) > 0);
                })
                .addSink(new DwdAppActiveSink(config))
                .name("DwdAppActiveSink")
                .uid("DwdAppActiveSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("DwdAppActiveJob");
    }

    @RequiredArgsConstructor
    private final static class DwdAppActiveSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DorisTemplate dorisSink;
        private DorisSchema schema;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisSink = new DorisTemplate("dorisSink");
            dorisSink.enableCache();
            List<String> derivedColumns = Arrays.asList(
                    "app_id2 = app_id2",
                    "pt = date_format(create_time, 'yyyy-MM-dd')",
                    "android_id = android_id",
                    "imei = imei",
                    "oaid = oaid",
                    "idfa = idfa",
                    "idfv = idfv",
                    "type = type",
                    "umeng_channel = umeng_channel",
                    "create_time = create_time",
                    "app_version = get_json_string(ads_header_json, '$.Version')",
                    "update_time = now()"
            );
            schema = DorisSchema.builder()
                    .database("test_db")
                    .tableName("dwd_app_active")
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
