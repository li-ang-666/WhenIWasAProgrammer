package com.liang.flink.project.bdp.equity.dao;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.liang.common.util.SqlUtils.formatValue;

public class BdpEquityDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("bdpEquity");

    public void deleteAll(String id, String companyId, String shareholderId) {
        //删除受益人
        String delete1 = new SQL()
                .DELETE_FROM("entity_beneficiary_details")
                .WHERE("id = " + formatValue(id))
                .toString();
        String delete2 = new SQL()
                .DELETE_FROM("entity_beneficiary_details")
                .WHERE("tyc_unique_entity_id = " + formatValue(companyId))
                .WHERE("tyc_unique_entity_id_beneficiary = " + formatValue(shareholderId))
                .toString();
        //删除控制人
        String delete3 = new SQL()
                .DELETE_FROM("entity_controller_details")
                .WHERE("id = " + formatValue(id))
                .toString();
        String delete4 = new SQL()
                .DELETE_FROM("entity_controller_details")
                .WHERE("company_id_controlled = " + formatValue(companyId))
                .WHERE("tyc_unique_entity_id = " + formatValue(shareholderId))
                .toString();
        //删除身份关系
        String delete5 = new SQL()
                .DELETE_FROM("shareholder_identity_type_details")
                .WHERE("tyc_unique_entity_id = " + formatValue(companyId))
                .WHERE("entity_type_id_with_shareholder_identity_type = " + formatValue(shareholderId))
                .toString();
        jdbcTemplate.update(delete1, delete2, delete3, delete4, delete5);
    }

    public String getEntityName(String entityId) {
        String sql = new SQL()
                .SELECT("entity_name_valid")
                .FROM("tyc_entity_main_reference")
                .WHERE("tyc_unique_entity_id = " + formatValue(entityId))
                .toString();
        String res = jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "";
    }

    public void replaceInto(String tableName, Map<String, Object> columnMap) {
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        String sql = new SQL().REPLACE_INTO(tableName)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
        jdbcTemplate.update(sql);
    }

    public void updateShareholderName(String shareholderId, String name) {
        String update1 = new SQL()
                .UPDATE("entity_beneficiary_details")
                .SET("entity_name_beneficiary = " + formatValue(name))
                .WHERE("tyc_unique_entity_id_beneficiary = " + formatValue(shareholderId))
                .toString();
        String update2 = new SQL()
                .UPDATE("entity_controller_details")
                .SET("entity_name_valid = " + formatValue(name))
                .WHERE("tyc_unique_entity_id = " + formatValue(shareholderId))
                .toString();
        jdbcTemplate.update(update1, update2);
    }

    public void updateCompanyName(String companyId, String companyName) {
        String update1 = new SQL()
                .UPDATE("entity_beneficiary_details")
                .SET("entity_name_valid = " + formatValue(companyName))
                .WHERE("tyc_unique_entity_id = " + formatValue(companyId))
                .toString();
        String update2 = new SQL()
                .UPDATE("entity_controller_details")
                .SET("company_name_controlled = " + formatValue(companyName))
                .WHERE("company_id_controlled = " + formatValue(companyId))
                .toString();
        jdbcTemplate.update(update1, update2);
    }

    public void replaceIntoRelation(List<Map<String, Object>> columnMaps) {
        List<String> sqls = new ArrayList<>();
        for (Map<String, Object> columnMap : columnMaps) {
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String sql = new SQL()
                    .REPLACE_INTO("shareholder_identity_type_details")
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sqls.add(sql);
            jdbcTemplate.update(sqls);
        }
    }
}
