package com.liang.flink.service.equity.controller;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EquityControllerDao {
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate dataListedCompany110 = new JdbcTemplate("110.data_listed_company");

    private final JdbcTemplate companyBase142 = new JdbcTemplate("142.company_base");
    private final JdbcTemplate humanBase040 = new JdbcTemplate("040.human_base");

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
                .SELECT("*")
                .FROM("stock_actual_controller")
                .WHERE("graph_id = " + SqlUtils.formatValue(companyId))
                .WHERE("is_deleted = 0")
                .toString();
        return dataListedCompany110.queryForColumnMaps(sql);
    }
}
