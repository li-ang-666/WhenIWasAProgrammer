package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.multi.node.MultiNodeService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Slf4j
@LocalConfigFile("multi-node.yml")
public class MultiNodeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> dataStream = StreamFactory.create(env);
        dataStream.rebalance().flatMap(new FlatMapFunction<SingleCanalBinlog, Input>() {
            @Override
            public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<Input> out) {
                String table = singleCanalBinlog.getTable();
                Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
                if (table.equals("entity_controller_details")) {
                    out.collect(new Input("control", String.valueOf(columnMap.get("tyc_unique_entity_id")), ""));
                    out.collect(new Input("control", String.valueOf(columnMap.get("company_id_controlled")), ""));
                } else if (table.equals("entity_beneficiary_details")) {
                    out.collect(new Input("benefit", String.valueOf(columnMap.get("tyc_unique_entity_id")), ""));
                    out.collect(new Input("benefit", String.valueOf(columnMap.get("tyc_unique_entity_id_beneficiary")), ""));
                } else {
                    out.collect(new Input("name", String.valueOf(columnMap.get("tyc_unique_entity_id")), String.valueOf(columnMap.get("entity_name_valid"))));
                }
            }
        }).setParallelism(config.getFlinkConfig().getOtherParallel()).keyBy(new KeySelector<Input, Input>() {
            @Override
            public Input getKey(Input input) {
                return input;
            }
        }).addSink(new MultiNodeSink(config)).setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("MultiNodeJob");
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public final static class Input implements Serializable {
        private String module;
        private String id;
        private String name;
    }

    @RequiredArgsConstructor
    private final static class MultiNodeSink extends RichSinkFunction<Input> {
        private final Config config;
        private MultiNodeService service;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new MultiNodeService();
        }

        @Override
        public void invoke(Input input, Context context) {
            List<String> sqls = service.invoke(input);
            for (String sql : sqls) {
                log.info("sql: {}", sql);
            }
        }
    }
}
