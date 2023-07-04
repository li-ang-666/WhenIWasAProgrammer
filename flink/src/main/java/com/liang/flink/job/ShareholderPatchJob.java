package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;
import java.util.Map;

@Slf4j
public class ShareholderPatchJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"shareholder-patch.yml"};
        }
        StreamExecutionEnvironment env = StreamEnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> sourceStream = StreamFactory.create(env);
        sourceStream.rebalance()
                .addSink(new Sink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("ShareholderPatchJob");
    }

    @Slf4j
    public static final class Sink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;
        private JdbcTemplate jdbcTemplateShareholder;

        public Sink(Config config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("bdpEquity");
            jdbcTemplateShareholder = new JdbcTemplate("shareholder");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) throws Exception {
            if (CanalEntry.EventType.DELETE == singleCanalBinlog.getEventType()) {
                return;
            }
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = String.valueOf(columnMap.get("id"));
            String companyId = String.valueOf(columnMap.get("tyc_unique_entity_id"));
            String shareholderId = String.valueOf(columnMap.get("tyc_unique_entity_id_beneficiary"));
            if (!StringUtils.isNumeric(companyId)) {
                companyId = getRepairEntityId(columnMap);
                columnMap.put("tyc_unique_entity_id", companyId);
            }
            // 检查 company_id 是不是有意义
            String checkInvestedCompanyNameSql = String.format("select company_name_invested from investment_relation where company_id_invested = %s -- and update_time >= '2023-01-01'", companyId);
            String checkInvestedCompanyName = jdbcTemplateShareholder.queryForObject(checkInvestedCompanyNameSql, rs -> rs.getString(1));
            if (checkInvestedCompanyName == null) {
                String deleteSql = String.format("delete from entity_beneficiary_details where tyc_unique_entity_id = '%s'", companyId);
                jdbcTemplate.update(deleteSql);
                return;
            }
            // 检查 shareholder 是不是有意义
            String checkShareholderSql = String.format("select entity_name_valid from tyc_entity_main_reference where tyc_unique_entity_id = '%s'", shareholderId);
            String checkedShareholderName = jdbcTemplate.queryForObject(checkShareholderSql, rs -> rs.getString(1));
            if (checkedShareholderName == null) {
                String deleteSql = String.format("delete from entity_beneficiary_details where id = %s", id);
                jdbcTemplate.update(deleteSql);
                return;
            }
            //两个都有意义,更新两个的名字
            columnMap.put("entity_name_valid", checkInvestedCompanyName);
            columnMap.put("entity_name_beneficiary", checkedShareholderName);
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String replaceSql = String.format("replace into entity_beneficiary_details(%s)values(%s)", insert.f0, insert.f1);
            jdbcTemplate.update(replaceSql);
        }

        @SuppressWarnings("unchecked")
        private String getRepairEntityId(Map<String, Object> columnMap) {
            String entityName = String.valueOf(columnMap.get("entity_name_valid"));
            List<Object> detail = JsonUtils.parseJsonArr(String.valueOf(columnMap.get("beneficiary_equity_relation_path_detail")));
            for (Object obj : detail) {
                for (Object o : (List<Object>) obj) {
                    Map<String, Object> properties = (Map<String, Object>) ((Map<String, Object>) o).get("properties");
                    if (properties.containsKey("name") && properties.containsKey("company_id")
                            && String.valueOf(properties.get("name")).equals(entityName)) {
                        return String.valueOf(properties.get("company_id"));
                    }
                }
            }
            return null;
        }
    }
}
