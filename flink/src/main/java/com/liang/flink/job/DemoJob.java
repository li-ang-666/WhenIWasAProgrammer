package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkSource;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.MemJdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TableNameUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.KafkaStreamFactory;
import com.liang.flink.high.level.api.RepairStreamFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DemoJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"demo.yml"};
        }
        StreamExecutionEnvironment env = StreamEnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        FlinkSource flinkSource = config.getFlinkSource();
        DataStream<SingleCanalBinlog> dataStream = flinkSource == FlinkSource.Kafka ?
                KafkaStreamFactory.create(env, 1) :
                RepairStreamFactory.create(env);
        dataStream
                .rebalance()
                .addSink(new DemoSink(config)).setParallelism(5);
        env.execute("DemoJob");
    }

    @Slf4j
    private static class DemoSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;

        public DemoSink(Config config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new MemJdbcTemplate("demoMemDruid");
        }

        @Override
        public void invoke(SingleCanalBinlog binlog, Context context) throws Exception {
            Map<String, Object> columnMap = binlog.getColumnMap();
            String createSQL = SqlUtils.columnList2Create(new ArrayList<>(columnMap.keySet()));
            String tableName = TableNameUtils.getRandomTableName();
            jdbcTemplate.update(String.format("create table %s(%s)", tableName, createSQL));
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            jdbcTemplate.batchUpdate(Collections.singletonList(String.format("insert into %s(%s) values(%s)", tableName, insert.f0, insert.f1)));
            String querySQL = String.format("select t1.* from %s t1 left join %s t2 on t1.id=t2.id", tableName, tableName);
            List<Map<String, Object>> columnMaps = jdbcTemplate.queryForColumnMaps(querySQL);
            log.info("print: {}", columnMaps);
            jdbcTemplate.update("drop table " + tableName);
        }
    }
}
