package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.extern.slf4j.Slf4j;
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
        DataStream<SingleCanalBinlog> sourceStream = StreamFactory.create(env);
        sourceStream.addSink(new Sink(ConfigUtils.getConfig()));
        env.execute();
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
            log.info("binlog: {}",singleCanalBinlog);
            if (CanalEntry.EventType.DELETE == singleCanalBinlog.getEventType()) {
                return;
            }
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String entityId = String.valueOf(columnMap.get("tyc_unique_entity_id"));
            String beneficiaryName = String.valueOf(columnMap.get("entity_name_beneficiary"));
            if ("null".equals(entityId) || "".equals(entityId)) {
                String repairEntityId = getRepairEntityId(columnMap);
                String id = String.valueOf(columnMap.get("id"));
                String sql = String.format("update entity_beneficiary_details set tyc_unique_entity_id = '%s' where id = %s", repairEntityId, id);
                log.info("sql: {}", sql);
            } else if ("null".equals(beneficiaryName) || "".equals(beneficiaryName)) {
                String repairEntityName = getRepairEntityName(columnMap);
                String id = String.valueOf(columnMap.get("id"));
                String entityName = String.valueOf(columnMap.get("entity_name_valid"));
                String sql = String.format("update entity_beneficiary_details set entity_name_valid = '%s', entity_name_beneficiary = '%s' where id = %s", repairEntityName, entityName, id);
                log.info("sql: {}", sql);
            }
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

        @SuppressWarnings("unchecked")
        private String getRepairEntityName(Map<String, Object> columnMap) {
            String entityId = String.valueOf(columnMap.get("tyc_unique_entity_id"));
            List<Object> detail = JsonUtils.parseJsonArr(String.valueOf(columnMap.get("beneficiary_equity_relation_path_detail")));
            for (Object obj : detail) {
                for (Object o : (List<Object>) obj) {
                    Map<String, Object> properties = (Map<String, Object>) ((Map<String, Object>) o).get("properties");
                    if (properties.containsKey("name") && properties.containsKey("company_id")
                            && String.valueOf(properties.get("company_id")).equals(entityId)) {
                        return String.valueOf(properties.get("name"));
                    }
                }
            }
            return null;
        }
    }
}
