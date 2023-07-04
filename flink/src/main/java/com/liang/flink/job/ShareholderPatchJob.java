package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.utils.BuildTab3Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.DELETE;

@Slf4j
public class ShareholderPatchJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"shareholder-patch.yml"};
        }
        StreamExecutionEnvironment env = StreamEnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> sourceStream = StreamFactory.create(env);
        sourceStream
                .keyBy(new KeySelector<SingleCanalBinlog, Object>() {
                    @Override
                    public Object getKey(SingleCanalBinlog singleCanalBinlog) throws Exception {
                        return singleCanalBinlog.getColumnMap().get("beneficiary_equity_relation_path_detail");
                    }
                })
                .addSink(new Sink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("ShareholderPatchJob");
    }

    @Slf4j
    public static final class Sink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;

        public Sink(Config config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("bdpEquity");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) throws Exception {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = String.valueOf(columnMap.get("id"));
            String companyId = String.valueOf(columnMap.get("company_id"));
            String shareholderId = String.valueOf(columnMap.get("shareholder_id"));
            String investmentRatioTotal = String.valueOf(columnMap.get("investment_ratio_total"));
            String isUltimate = String.valueOf(columnMap.get("is_ultimate"));
            String equityHoldingPath = String.valueOf(columnMap.get("equity_holding_path"));
            String isDeleted = String.valueOf(columnMap.get("is_deleted"));
            if (!"1".equals(isUltimate)) {
                return;
            }
            CanalEntry.EventType eventType = singleCanalBinlog.getEventType();
            if (DELETE == eventType || "1".equals(isDeleted)) {
                jdbcTemplate.update("delete from entity_beneficiary_details where id = " + id);
                return;
            }
            HashMap<String, Object> resultMap = new HashMap<>();
            resultMap.put("id", id);
            resultMap.put("tyc_unique_entity_id", companyId);
            resultMap.put("tyc_unique_entity_id_beneficiary", shareholderId);
            String companyName = jdbcTemplate.queryForObject(String.format("select entity_name_valid from tyc_entity_main_reference where tyc_unique_entity_id = '%s'", companyId), rs -> rs.getString(1));
            resultMap.put("entity_name_valid", companyName);
            String shareholderName = jdbcTemplate.queryForObject(String.format("select entity_name_valid from tyc_entity_main_reference where tyc_unique_entity_id = '%s'", shareholderId), rs -> rs.getString(1));
            resultMap.put("entity_name_beneficiary", shareholderName);
            BuildTab3Path.PathNode pathNode = BuildTab3Path.buildTab3PathSafe(shareholderId, equityHoldingPath);
            resultMap.put("equity_relation_path_cnt", pathNode.getCount());
            resultMap.put("beneficiary_equity_relation_path_detail", pathNode.getPathStr());
            resultMap.put("estimated_equity_ratio_total", investmentRatioTotal);
            resultMap.put("beneficiary_validation_time_year", 2023);
            resultMap.put("entity_type_id", 1);
            String deleteSql1 = String.format("delete from entity_beneficiary_details where id = %s", id);
            String deleteSql2 = String.format("delete from entity_beneficiary_details where tyc_unique_entity_id = '%s' and tyc_unique_entity_id_beneficiary = '%s'", companyId, shareholderId);
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String replaceSql = String.format("replace into entity_beneficiary_details(%s)values(%s)", insert.f0, insert.f1);
            jdbcTemplate.update(deleteSql1, deleteSql2, replaceSql);
        }
    }
}
