package com.liang.flink.project.multi.node;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;

import java.util.List;

public class MultiNodeDao {
    private final static String NAME_SOURCE = "tyc_entity_main_reference";
    private final static String CONTROL_SOURCE = "entity_controller_details";
    private final static String BENEFIT_SOURCE = "entity_beneficiary_details";
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("463.bdp_equity");

    public String getName(Object tycUniqueEntityId) {
        String sql = new SQL().SELECT("entity_name_valid")
                .FROM(NAME_SOURCE)
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                .toString();
        return jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
    }

    public List<String> getControlJsonByCompanyId(Object tycUniqueEntityId) {
        String sql = new SQL().SELECT("controlling_equity_relation_path_detail")
                .FROM(CONTROL_SOURCE)
                .WHERE("company_id_controlled = " + SqlUtils.formatValue(tycUniqueEntityId))
                .toString();
        return jdbcTemplate.queryForList(sql, rs -> rs.getString(1));
    }

    public List<String> getControlJsonByShareholderId(Object tycUniqueEntityId) {
        String sql = new SQL().SELECT("controlling_equity_relation_path_detail")
                .FROM(CONTROL_SOURCE)
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                .toString();
        return jdbcTemplate.queryForList(sql, rs -> rs.getString(1));
    }

    public List<String> getBenefitJsonByCompanyId(Object tycUniqueEntityId) {
        String sql = new SQL().SELECT("beneficiary_equity_relation_path_detail")
                .FROM(BENEFIT_SOURCE)
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                .toString();
        return jdbcTemplate.queryForList(sql, rs -> rs.getString(1));
    }

    public List<String> getBenefitJsonByShareholderId(Object tycUniqueEntityId) {
        String sql = new SQL().SELECT("beneficiary_equity_relation_path_detail")
                .FROM(BENEFIT_SOURCE)
                .WHERE("tyc_unique_entity_id_beneficiary = " + SqlUtils.formatValue(tycUniqueEntityId))
                .toString();
        return jdbcTemplate.queryForList(sql, rs -> rs.getString(1));
    }
}
