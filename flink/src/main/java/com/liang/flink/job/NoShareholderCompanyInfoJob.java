package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.CanalBinlogStreamFactory;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NoShareholderCompanyInfoJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = new String[]{"no-shareholder-company-info.yml"};
        StreamExecutionEnvironment streamEnvironment = StreamEnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = CanalBinlogStreamFactory.create(streamEnvironment);
        stream
                .shuffle()
                .addSink(new MySqlSink(config))
                .name("MySqlSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        streamEnvironment.execute("NoShareholderCompanyInfoJob");
    }

    @Slf4j
    private final static class MySqlSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private final List<String> cache = new ArrayList<>();
        private DataUpdateService<Map<String, Object>> service;
        private JdbcTemplate jdbcTemplate;

        public MySqlSink(Config config) throws Exception {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            DataUpdateContext<Map<String, Object>> context = new DataUpdateContext<Map<String, Object>>("com.liang.flink.project.no.thareholder.company.info.impl")
                    .addClass("CompanyIndex")
                    .addClass("StockActualController")
                    .addClass("CompanyLegalPerson")
                    .addClass("CompanyBondPlates")
                    .addClass("CompanyEquityRelationDetails");
            service = new DataUpdateService<>(context);
            jdbcTemplate = new JdbcTemplate("sink");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) throws Exception {
            List<Map<String, Object>> columnMaps = service.invoke(singleCanalBinlog);
            for (Map<String, Object> columnMap : columnMaps) {
                Tuple2<String, String> insertSyntax = SqlUtils.columnMap2Insert(columnMap);
                String sql = String.format("replace into no_shareholder_company_info(%s) values(%s)", insertSyntax.f0, insertSyntax.f1);
                cache.add(sql);
            }
            FlinkConfig.SourceType sourceType = ConfigUtils.getConfig().getFlinkConfig().getSourceType();
            if (sourceType == FlinkConfig.SourceType.Kafka || cache.size() >= 2048) {
                jdbcTemplate.batchUpdate(cache);
                cache.clear();
            }
        }

        @Override
        public void close() throws Exception {
            ConfigUtils.closeAll();
        }
    }
}
