package com.liang.flink.project.bdp.equity.dao;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

import static com.liang.common.util.SqlUtils.formatValue;

public class BdpEquityDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("bdpEquity");

    public void deleteAll(String id, String companyId, String shareholderId) {
        String delete1 = new SQL()
                .DELETE_FROM("entity_beneficiary_details")
                .WHERE("id = " + formatValue(id))
                .toString();
        String delete2 = new SQL()
                .DELETE_FROM("entity_beneficiary_details")
                .WHERE("tyc_unique_entity_id = " + formatValue(companyId))
                .WHERE("tyc_unique_entity_id_beneficiary = " + formatValue(shareholderId))
                .toString();
        String delete3 = new SQL()
                .DELETE_FROM("entity_controller_details")
                .WHERE("id = " + formatValue(id))
                .toString();
        String delete4 = new SQL()
                .DELETE_FROM("entity_controller_details")
                .WHERE("company_id_controlled = " + formatValue(companyId))
                .WHERE("tyc_unique_entity_id = " + formatValue(shareholderId))
                .toString();
        jdbcTemplate.update(delete1, delete2, delete3, delete4);
    }

    public String getEntityName(String entityId) {
        String sql = new SQL()
                .SELECT("entity_name_valid")
                .FROM("tyc_entity_main_reference")
                .WHERE("tyc_unique_entity_id = " + formatValue(entityId))
                .toString();
        return jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
    }

    public void replaceInto(String tableName, Map<String, Object> columnMap) {
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        String sql = new SQL().REPLACE_INTO(tableName)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
        jdbcTemplate.update(sql);
    }
}
