package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.project.evaluation.institution.candidate.EvaluationInstitutionCandidateService;
import com.liang.flink.project.evaluation.institution.candidate.impl.*;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateImpl;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@LocalConfigFile("evaluation-institution-candidate.yml")
public class EvaluationInstitutionCandidateJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        Distributor distributor = new Distributor()
                .with("zhixinginfo_evaluate", e -> String.valueOf(e.getColumnMap().get("id")))
                .with("zhixinginfo_evaluate_index", e -> String.valueOf(e.getColumnMap().get("main_id")))
                .with("company_law_human_realtion", e -> String.valueOf(e.getColumnMap().get("source_id")))
                .with("zhixinginfo_evaluate_result", e -> String.valueOf(e.getColumnMap().get("id")))
                .with("enterprise", e -> String.valueOf(e.getColumnMap().get("graph_id")));
        // stream流
        stream.keyBy(distributor)
                .addSink(new EvaluationInstitutionCandidateSink(config)).name("EvaluationInstitutionCandidateSink").setParallelism(config.getFlinkConfig().getOtherParallel());
        // 触发obs
        if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.Kafka) {
            DaemonExecutor.launch("UpdateEvaluateResultAll", new Runnable() {
                private long lastUpdateTime = System.currentTimeMillis();

                @SneakyThrows(InterruptedException.class)
                @Override
                public void run() {
                    JdbcTemplate trigger = new JdbcTemplate("108.data_judicial_risk");
                    JdbcTemplate sink = new JdbcTemplate("427.test");
                    while (true) {
                        long current = System.currentTimeMillis();
                        if (current - lastUpdateTime >= 1000 * 60 * 2) {
                            log.info("trigger run once");
                            String sql1 = new SQL().UPDATE(EvaluationInstitutionCandidateService.TABLE)
                                    .SET("is_eventual_evaluation_institution = 0")
                                    .WHERE("is_eventual_evaluation_institution = 1")
                                    .toString();
                            sink.update(sql1);
                            String sql2 = new SQL().UPDATE("zhixinginfo_evaluate_result")
                                    .SET("property5 = " + SqlUtils.formatValue(UUID.randomUUID()))
                                    .WHERE("reference_mode in ('委托评估','定向询价')")
                                    .WHERE("deleted = 0")
                                    .WHERE("caseNumber is not null and caseNumber <> ''")
                                    .WHERE("subjectName is not null and subjectName <> ''")
                                    .WHERE("result_text_oss is not null and result_text_oss <>''")
                                    .toString();
                            trigger.update(sql2);
                            lastUpdateTime = current;
                            TimeUnit.MINUTES.sleep(1440);
                        }
                        TimeUnit.MINUTES.sleep(1);
                    }
                }
            });
        }
        env.execute("EvaluationInstitutionCandidateJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    @DataUpdateImpl({
            ZhixinginfoEvaluate.class,
            ZhixinginfoEvaluateIndex.class,
            CompanyLawHumanRealtion.class,
            Enterprise.class,
            ZhixinginfoEvaluateResult.class
    })
    private final static class EvaluationInstitutionCandidateSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DataUpdateService<String> service;
        private JdbcTemplate sink;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new DataUpdateService<>(new DataUpdateContext<>(EvaluationInstitutionCandidateSink.class));
            sink = new JdbcTemplate("427.test");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            sink.update(service.invoke(singleCanalBinlog));
        }

        @Override
        public void close() {
            ConfigUtils.unloadAll();
        }
    }
}
