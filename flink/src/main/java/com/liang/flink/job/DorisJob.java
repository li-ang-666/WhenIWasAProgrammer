package com.liang.flink.job;

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
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

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
            dorisSink.enableCache(3000, 1024);
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
}
