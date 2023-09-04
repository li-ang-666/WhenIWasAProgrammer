package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.evaluation.institution.candidate.EvaluationInstitutionCandidateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@LocalConfigFile("evaluation-institution-candidate.yml")
public class EvaluationInstitutionCandidateJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new EvaluationInstitutionCandidateSink(config)).name("EvaluationInstitutionCandidateSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("EvaluationInstitutionCandidateJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class EvaluationInstitutionCandidateSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Set<String> evaluationIds = ConcurrentHashMap.newKeySet();
        private final Config config;
        private EvaluationInstitutionCandidateService service;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new EvaluationInstitutionCandidateService();
            jdbcTemplate = new JdbcTemplate("427.test");
            jdbcTemplate.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            String gid = String.valueOf(singleCanalBinlog.getColumnMap().get("id"));
            if (TycUtils.isUnsignedId(gid)) {
                synchronized (evaluationIds) {
                    evaluationIds.add(gid);
                }
            }
            if (evaluationIds.size() >= 1) {
                flush();
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
            ConfigUtils.unloadAll();
        }

        private void flush() {
            synchronized (evaluationIds) {
                for (String companyGid : evaluationIds) {
                    List<String> sqls = service.invoke(companyGid);
                    jdbcTemplate.update(sqls);
                }
                evaluationIds.clear();
            }
        }
    }
}
