package com.liang.flink.job;

import cn.hutool.core.io.IoUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BfsRepairJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(null);
        Config config = ConfigUtils.getConfig();
        env.addSource(new BfsRepairSource())
                .setParallelism(1)
                .name("BfsRepairSource")
                .uid("BfsRepairSource")
                .rebalance()
                .addSink(new BfsRepairSink(config))
                .setParallelism(1)
                .name("BfsRepairSink")
                .uid("BfsRepairSink");
        env.execute("BfsRepairJob");
    }

    private static final class BfsRepairSource extends RichSourceFunction<String> {
        private final AtomicBoolean canceled = new AtomicBoolean(false);

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            InputStream resourceAsStream = BfsRepairJob.class.getClassLoader().getResourceAsStream("wrong-company-ids.txt");
            String[] companyIds = IoUtil.readUtf8(resourceAsStream).split("\n");
            while (!canceled.get() && i < companyIds.length) {
                ctx.collect(companyIds[i++]);
                TimeUnit.SECONDS.sleep(2);
            }
        }

        @Override
        public void cancel() {
            canceled.set(true);
        }
    }

    @RequiredArgsConstructor
    private static final class BfsRepairSink extends RichSinkFunction<String> {
        private final Config config;
        private JdbcTemplate graphData430;
        private JdbcTemplate prism116;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            graphData430 = new JdbcTemplate("430.graph_data");
            prism116 = new JdbcTemplate("116.prism");
        }

        @Override
        public void invoke(String companyId, Context context) {
            String deleteSql = new SQL().DELETE_FROM("company_equity_relation_details")
                    .WHERE("company_id_invested = " + SqlUtils.formatValue(companyId))
                    .toString();
            graphData430.update(deleteSql);
            String updateSql = new SQL().UPDATE("equity_ratio")
                    .SET("update_time = now()")
                    .WHERE("company_graph_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            prism116.update(updateSql);
        }
    }
}

