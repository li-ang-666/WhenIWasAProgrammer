package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

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
        private ObsWriter obsWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            obsWriter = new ObsWriter("obs://hadoop-obs/flink_tmp/graph/", ObsWriter.FileFormat.TXT);
            obsWriter.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String stringBuilder = String.valueOf(columnMap.get("tyc_unique_entity_id_investor")).replaceAll("[\n\u0001,]", "") +
                    "," +
                    String.valueOf(columnMap.get("company_id_invested")).replaceAll("[\n\u0001,]", "") +
                    "," +
                    "equity_relation" +
                    "," +
                    String.valueOf(columnMap.get("equity_ratio")).replaceAll("[\n\u0001,]", "") +
                    "," +
                    String.valueOf(columnMap.get("equity_amount")).replaceAll("[\n\u0001,]", "") +
                    "," +
                    String.valueOf(columnMap.get("equity_amount_currency")).replaceAll("[\n\u0001,]", "") +
                    "," +
                    "2024" +
                    "," +
                    "1704038400000";
            obsWriter.update(stringBuilder);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            obsWriter.flush();
        }

        @Override
        public void finish() {
            obsWriter.flush();
        }

        @Override
        public void close() {
            obsWriter.flush();
        }
    }
}
