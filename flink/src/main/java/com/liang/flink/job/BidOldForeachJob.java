package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@LocalConfigFile("bid-old-foreach.yml")
public class BidOldForeachJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new BidOldForeachSink(config))
                .name("BidOldForeachSink")
                .uid("BidOldForeachSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("BidOldForeachJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class BidOldForeachSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        JdbcTemplate jdbcTemplate;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("104.data_bid");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Object id = singleCanalBinlog.getColumnMap().get("id");
            String sql = new SQL().UPDATE("company_bid")
                    .SET("update_time = now()")
                    .WHERE("id = " + SqlUtils.formatValue(id))
                    .toString();
            jdbcTemplate.update(sql);
        }
    }
}
