package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.evaluation.institution.candidate.impl.CompanyLawHumanRealtion;
import com.liang.flink.project.evaluation.institution.candidate.impl.Enterprise;
import com.liang.flink.project.evaluation.institution.candidate.impl.ZhixinginfoEvaluate;
import com.liang.flink.project.evaluation.institution.candidate.impl.ZhixinginfoEvaluateIndex;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateImpl;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@Slf4j
@LocalConfigFile("evaluation-institution-candidate.yml")
public class EvaluationInstitutionCandidateJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        Distributor distributor = new Distributor()
                .with("zhixinginfo_evaluate", e -> String.valueOf(e.getColumnMap().get("id")))
                .with("zhixinginfo_evaluate", e -> String.valueOf(e.getColumnMap().get("main_id")))
                .with("company_law_human_realtion", e -> String.valueOf(e.getColumnMap().get("source_id")))
                .with("enterprise", e -> String.valueOf(e.getColumnMap().get("id")));
        stream.keyBy(distributor)
                .addSink(new EvaluationInstitutionCandidateSink(config)).name("EvaluationInstitutionCandidateSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("EvaluationInstitutionCandidateJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    @DataUpdateImpl({
            ZhixinginfoEvaluate.class,
            ZhixinginfoEvaluateIndex.class,
            CompanyLawHumanRealtion.class,
            Enterprise.class,
    })
    private final static class EvaluationInstitutionCandidateSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DataUpdateService<String> service;
        private JdbcTemplate jdbcTemplate;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new DataUpdateService<>(new DataUpdateContext<>(EvaluationInstitutionCandidateSink.class));
            jdbcTemplate = new JdbcTemplate("427.test");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            jdbcTemplate.update(service.invoke(singleCanalBinlog));
        }

        @Override
        public void close() {
            ConfigUtils.unloadAll();
        }
    }
}
