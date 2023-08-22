package com.liang.flink.project.company.base.info;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompanyBaseInfoDao {
    private final JdbcTemplate prism464 = new JdbcTemplate("464.prism");
    private final JdbcTemplate prism116 = new JdbcTemplate("116.prism");
    private final JdbcTemplate companyBase465 = new JdbcTemplate("465.company_base");
    private final JdbcTemplate gov = new JdbcTemplate("111.data_experience_situation");

    public Map<String, Object> queryEnterprise(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return new HashMap<>();
        }
        String sql = new SQL()
                .SELECT("*")
                .FROM("enterprise")
                .WHERE("deleted = 0")
                .WHERE("id = " + SqlUtils.formatValue(companyCid))
                .toString();
        List<Map<String, Object>> columnMaps = prism464.queryForColumnMaps(sql);
        if (columnMaps.isEmpty()) {
            return new HashMap<>();
        }
        return columnMaps.get(0);
    }

    public Tuple4<String, String, String, String> getEquityInfo(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return Tuple4.of("", "", "", "");
        }
        String sql = new SQL()
                .SELECT("ifnull(reg_capital_amount,'')", "ifnull(reg_capital_currency,'')", "ifnull(actual_capital_amount,'')", "ifnull(actual_capital_currency,'')")
                .FROM("company_clean_info")
                .WHERE("is_deleted = 0")
                .WHERE("id = " + SqlUtils.formatValue(companyCid))
                .toString();
        Tuple4<String, String, String, String> res = prism116.queryForObject(sql, rs -> Tuple4.of(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
        return res != null ? res : Tuple4.of("", "", "", "");
    }

    public String getProperty(String companyGid) {
        return "这是一个property";
        /*if (!TycUtils.isUnsignedId(companyGid)) {
            return "0";
        }
        String sql = new SQL()
                .SELECT("entity_property")
                .FROM("tyc_entity_general_property_reference")
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        String res = companyBase465.queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "0";*/
    }

    public String getTax(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return "";
        }
        String sql = new SQL()
                .SELECT("property4")
                .FROM("company")
                .WHERE("id = " + SqlUtils.formatValue(companyCid))
                .toString();
        String res = prism116.queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "";
    }

    public Map<String, Object> queryGovInfo(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return new HashMap<>();
        }
        String sql = new SQL()
                .SELECT("*")
                .FROM("gov_unit")
                .WHERE("is_deleted = 0")
                .WHERE("company_id = " + SqlUtils.formatValue(companyCid))
                .toString();
        List<Map<String, Object>> columnMaps = gov.queryForColumnMaps(sql);
        if (columnMaps.isEmpty()) {
            return new HashMap<>();
        }
        return columnMaps.get(0);
    }
}
