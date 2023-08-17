package com.liang.flink.project.bdp.equity.dao;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;

import static com.liang.common.util.SqlUtils.formatValue;

public class BdpEquityDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("463.bdp_equity");

    public void updateShareholderName(String shareholderId, String shareholderName) {
        String update1 = new SQL()
                .UPDATE("entity_beneficiary_details")
                .SET("entity_name_beneficiary = " + formatValue(shareholderName))
                .WHERE("tyc_unique_entity_id_beneficiary = " + formatValue(shareholderId))
                .toString();
        String update2 = new SQL()
                .UPDATE("entity_controller_details")
                .SET("entity_name_valid = " + formatValue(shareholderName))
                .WHERE("tyc_unique_entity_id = " + formatValue(shareholderId))
                .toString();
        String update3 = new SQL()
                .UPDATE("shareholder_identity_type_details")
                .SET("entity_name_valid_with_shareholder_identity_type = " + formatValue(shareholderName))
                .WHERE("tyc_unique_entity_id_with_shareholder_identity_type = " + formatValue(shareholderId))
                .toString();
        jdbcTemplate.update(update1, update2, update3);
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
        String update3 = new SQL()
                .UPDATE("shareholder_identity_type_details")
                .SET("entity_name_valid = " + formatValue(companyName))
                .WHERE("tyc_unique_entity_id = " + formatValue(companyId))
                .toString();
        jdbcTemplate.update(update1, update2, update3);
    }
}
