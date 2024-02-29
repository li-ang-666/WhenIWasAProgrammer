package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Map;

@LocalConfigFile("equity-control-count.yml")
public class EquityControlCountJob {
    private static final String QUERY_SOURCE = "427.test";
    private static final String QUERY_TABLE = "bdp_equity.entity_controller_details_new";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .flatMap(new EquityControlCountFlatMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityControlCountFlatMapper")
                .uid("EquityControlCountFlatMapper")
                .keyBy(e -> e)
                .addSink(new EquityControlCountSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityControlCountSink")
                .uid("EquityControlCountSink");
        env.execute("EquityControlCountJob");
    }

    @RequiredArgsConstructor
    private static final class EquityControlCountFlatMapper extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            out.collect(String.valueOf(columnMap.get("tyc_unique_entity_id")));
            out.collect(String.valueOf(columnMap.get("company_id_controlled")));
        }
    }

    @RequiredArgsConstructor
    private static final class EquityControlCountSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate jdbcTemplate;
        private HbaseTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate(QUERY_SOURCE);
            sink = new HbaseTemplate("hbaseSink");
            sink.enableCache();
        }

        @Override
        public void invoke(String tycUniqueEntityId, Context context) {
            if (!TycUtils.isTycUniqueEntityId(tycUniqueEntityId)) return;
            if (TycUtils.isUnsignedId(tycUniqueEntityId)) {
                // 公司详情页-实控人count
                String queryControllerCountSql = new SQL()
                        .SELECT("count(1)")
                        .FROM(QUERY_TABLE)
                        .WHERE("company_id_controlled = " + SqlUtils.formatValue(tycUniqueEntityId))
                        .WHERE("is_controller_tyc_unique_entity_id = '1'")
                        .toString();
                String controllerCount = jdbcTemplate.queryForObject(queryControllerCountSql, rs -> rs.getString(1));
                sink.update(new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, tycUniqueEntityId, Collections.singletonMap("has_controller", controllerCount)));
                // 公司详情页-实控权count
                String queryControlCountSql = new SQL()
                        .SELECT("count(1)")
                        .FROM(QUERY_TABLE)
                        .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                        .toString();
                String controlCount = jdbcTemplate.queryForObject(queryControlCountSql, rs -> rs.getString(1));
                sink.update(new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, tycUniqueEntityId, Collections.singletonMap("num_control_ability", controlCount)));
            } else {
                // 老板详情页-实控权count
                String queryControlCountSql = new SQL()
                        .SELECT("count(1)")
                        .FROM(QUERY_TABLE)
                        .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                        .toString();
                String controlCount = jdbcTemplate.queryForObject(queryControlCountSql, rs -> rs.getString(1));
                sink.update(new HbaseOneRow(HbaseSchema.HUMAN_ALL_COUNT, tycUniqueEntityId, Collections.singletonMap("num_control_ability", controlCount)));
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            sink.flush();
        }

        @Override
        public void finish() {
            sink.flush();
        }

        @Override
        public void close() {
            sink.flush();
        }
    }
}
