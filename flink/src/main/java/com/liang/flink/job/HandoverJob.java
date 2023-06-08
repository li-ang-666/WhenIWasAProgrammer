package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.KafkaSourceStreamFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

public class HandoverJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = new String[]{"handover-config.yml"};
        StreamExecutionEnvironment env = StreamEnvironmentFactory.create(args);
        DataStream<SingleCanalBinlog> binlogDataStream = KafkaSourceStreamFactory.create(env);
        binlogDataStream.addSink(new Sink(ConfigUtils.getConfig()));
        env.execute();
    }

    private static class Sink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;

        public Sink(Config config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("bigdataOnline");
        }

        @Override
        public void invoke(SingleCanalBinlog value, Context context) throws Exception {
            CanalEntry.EventType eventType = value.getEventType();
            Map<String, Object> columnMap = value.getColumnMap();
            String secondaryDimName = String.valueOf(columnMap.get("visit_secondary_dim_name"));
            if (eventType != CanalEntry.EventType.DELETE && "最终受益人".equals(secondaryDimName)) {
                String id = String.valueOf(columnMap.get("id"));
                String sql = String.format("update itch_point_secondary_dims_uv_count set visit_secondary_dim_name = '最终受益人' where id = %s", id);
                jdbcTemplate.update(sql);
            }
        }
    }
}