package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

//  -- beeline
//  use test;
//  drop table if exists graph_export;
//  create table if not exists graph_export(
//    `row` string
//  )stored as textfile location 'obs://hadoop-obs/flink';

//  -- 建表后再写入数据
//  select count(1) from graph_export;

//  -- spark-sql
//  use test;
//  insert overwrite table graph_export select /*+ REPARTITION(1) */ * from graph_export;
//  select count(1) from graph_export;
@LocalConfigFile("graph-export.yml")
public class GraphExportJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new GraphExportSink(config))
                .name("GraphExportSink")
                .uid("GraphExportSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("GraphExportJob");
    }

    @RequiredArgsConstructor
    private final static class GraphExportSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private ObsWriter edgeObsWriter;
        private ObsWriter nodeObsWriter;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            edgeObsWriter = new ObsWriter("obs://hadoop-obs/flink/graph/edge/", ObsWriter.FileFormat.TXT);
            edgeObsWriter.enableCache();
            nodeObsWriter = new ObsWriter("obs://hadoop-obs/flink/graph/node/", ObsWriter.FileFormat.TXT);
            nodeObsWriter.enableCache();
            jdbcTemplate = new JdbcTemplate("157.prism_boss");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            Tuple2<String, String> node = getNode(columnMap);
            if (node == null) {
                return;
            }
            nodeObsWriter.update(node.f0);
            nodeObsWriter.update(node.f1);
            edgeObsWriter.update(getEdge(columnMap));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            edgeObsWriter.flush();
        }

        @Override
        public void finish() {
            edgeObsWriter.flush();
        }

        @Override
        public void close() {
            edgeObsWriter.flush();
        }

        private String getEdge(Map<String, Object> columnMap) {
            List<String> edge = Arrays.asList(
                    String.valueOf(columnMap.get("tyc_unique_entity_id_investor")).replaceAll("[\n,]", ""),
                    String.valueOf(columnMap.get("company_id_invested")).replaceAll("[\n,]", ""),
                    "equity_relation",
                    String.valueOf(columnMap.get("equity_ratio")).replaceAll("[\n,]", ""),
                    String.valueOf(columnMap.get("equity_amount")).replaceAll("[\n,]", ""),
                    String.valueOf(columnMap.get("equity_amount_currency")).replaceAll("[\n,]", ""),
                    String.valueOf(columnMap.get("reference_pt_year")).replaceAll("[\n,]", ""),
                    "1704038400000"
            );
            return String.join(",", edge);
        }

        private Tuple2<String, String> getNode(Map<String, Object> columnMap) {
            List<String> company = Arrays.asList(
                    String.valueOf(columnMap.get("company_id_invested")).replaceAll("[\n,]", ""),
                    "node",
                    "2",
                    String.valueOf(columnMap.get("company_id_invested")).replaceAll("[\n,]", ""),
                    "0",
                    String.valueOf(columnMap.get("tyc_unique_entity_name_invested")).replaceAll("[\n,]", ""),
                    "",
                    "true",
                    "1704038400000"
            );
            String type = String.valueOf(columnMap.get("investor_identity_type"));
            List<String> shareholder;
            // 公司
            if ("2".equals(type)) {
                shareholder = Arrays.asList(
                        String.valueOf(columnMap.get("company_id_investor")).replaceAll("[\n,]", ""),
                        "node",
                        "2",
                        String.valueOf(columnMap.get("company_id_investor")).replaceAll("[\n,]", ""),
                        "0",
                        String.valueOf(columnMap.get("tyc_unique_entity_name_investor")).replaceAll("[\n,]", ""),
                        "",
                        "true",
                        "1704038400000"
                );
            }
            // 人
            else if ("1".equals(type)) {
                String sql = "select company_graph_id from company_human_relation where human_pid = " + SqlUtils.formatValue(columnMap.get("tyc_unique_entity_id_investor"));
                String res = jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
                shareholder = Arrays.asList(
                        String.valueOf(columnMap.get("tyc_unique_entity_id_investor")).replaceAll("[\n,]", ""),
                        "node",
                        "1",
                        ((res != null) ? res : "0"),
                        String.valueOf(columnMap.get("company_id_investor")).replaceAll("[\n,]", ""),
                        String.valueOf(columnMap.get("tyc_unique_entity_name_investor")).replaceAll("[\n,]", ""),
                        "",
                        "true",
                        "1704038400000"
                );
            } else {
                return null;
            }
            return Tuple2.of(String.join(",", company), String.join(",", shareholder));
        }
    }
}
