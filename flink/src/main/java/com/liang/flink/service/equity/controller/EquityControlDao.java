package com.liang.flink.service.equity.controller;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EquityControlDao {
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate dataListedCompany110 = new JdbcTemplate("110.data_listed_company");
    private final JdbcTemplate prismShareholderPath457 = new JdbcTemplate("427.test");
    private final JdbcTemplate companyBase465 = new JdbcTemplate("465.company_base");

    public Map<String, Object> queryCompanyInfo(String companyId) {
        String sql = new SQL()
                .SELECT("*")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        List<Map<String, Object>> columnMaps = companyBase435.queryForColumnMaps(sql);
        return columnMaps.isEmpty() ? new HashMap<>() : columnMaps.get(0);
    }

    public List<Map<String, Object>> queryListedAnnouncedControllers(String companyId) {
        String sql = new SQL()
                .SELECT("case when controller_type = 1 then controller_gid when controller_type = 0 then controller_pid else 0 end id")
                .SELECT("holding_ratio")
                .FROM("stock_actual_controller")
                .WHERE("graph_id = " + SqlUtils.formatValue(companyId))
                .WHERE("is_deleted = 0")
                .toString();
        return dataListedCompany110.queryForColumnMaps(sql);
    }

    public List<Map<String, Object>> queryRatioPathCompany(String companyId) {
        String sql = new SQL()
                .SELECT("*")
                .FROM("prism_shareholder_path.ratio_path_company_new")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("company_id != shareholder_id")
                .toString();
        return prismShareholderPath457.queryForColumnMaps(sql);
    }

    public boolean isPartnership(String companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("tyc_entity_general_property_reference")
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(companyId))
                .WHERE("entity_property in (15,16)")
                .toString();
        String res = companyBase465.queryForObject(sql, rs -> rs.getString(1));
        return res != null;
    }

    public List<Map<String, Object>> queryAllPersonnels(String companyId) {
        String sql = new SQL()
                .SELECT("human_id id")
                .FROM("personnel")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("personnel_position like '%董事长%'")
                .toString();
        return companyBase435.queryForColumnMaps(sql);
    }

    public List<Map<String, Object>> queryAllPartners(String companyId) {
        String sql = new SQL()
                .SELECT("case when legal_rep_type = 1 then legal_rep_name_id when legal_rep_type = 2 then legal_rep_human_id else 0 end id")
                .FROM("company_legal_person")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return companyBase435.queryForColumnMaps(sql);
    }

    public String queryIsPersonnel(String companyId, String humanId) {
        String sql = new SQL()
                .SELECT("personnel_position")
                .FROM("personnel")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("(personnel_position like '%董事长%' or personnel_position like '%执行董事%')")
                .WHERE("human_id = " + SqlUtils.formatValue(humanId))
                .toString();
        return companyBase435.queryForObject(sql, rs -> rs.getString(1));
    }
}
