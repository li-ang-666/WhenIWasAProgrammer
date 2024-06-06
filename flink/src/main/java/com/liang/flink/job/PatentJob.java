package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@LocalConfigFile("patent.yml")
public class PatentJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> dataStream = StreamFactory.create(env);
        Distributor distributor = new Distributor();
        distributor
                .with("company_patent_basic_info_index", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("id")))
                .with("company_patent_basic_info_index_split", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("company_id")));
        dataStream
                .keyBy(distributor)
                .addSink(new PatentSink(config))
                .name("PatentSink")
                .uid("PatentSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("PatentJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class PatentSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate("451.intellectual_property_info");
            sink.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            switch (singleCanalBinlog.getTable()) {
                case "company_patent_basic_info_index":
                    parseIndex(singleCanalBinlog);
                    break;
                case "company_patent_basic_info_index_split":
                    parseIndexSplit(singleCanalBinlog);
                    break;
                default:
                    break;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
        }

        @Override
        public void finish() {
            flush();
        }

        @Override
        public void close() {
            flush();
        }

        private void flush() {
            sink.flush();
        }

        private void parseIndex(SingleCanalBinlog singleCanalBinlog) {

        }

        private void parseIndexSplit(SingleCanalBinlog singleCanalBinlog) {

        }
    }
}
