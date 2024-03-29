package com.liang.flink.project.company.base.info;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompanyBaseInfoDao {
    private final static Map<String, String> dictionary = new HashMap<>();

    static {
        dictionary.put("0", "其它组织");
        dictionary.put("1", "工商来源其它企业");
        dictionary.put("2", "香港");
        dictionary.put("3", "台湾");
        dictionary.put("4", "中央级事业单位");
        dictionary.put("5", "其他事业单位");
        dictionary.put("6", "律所");
        dictionary.put("7", "社会组织");
        dictionary.put("8", "基金会");
        dictionary.put("9", "机构");
        dictionary.put("10", "国家机关");
        dictionary.put("11", "工商来源个体工商户");
        dictionary.put("12", "农民专业合作社");
        dictionary.put("13", "工商来源有限责任公司");
        dictionary.put("14", "工商来源股份有限公司");
        dictionary.put("15", "工商来源普通合伙企业");
        dictionary.put("16", "工商来源有限合伙企业");
        dictionary.put("17", "工商来源个人独资企业");
        dictionary.put("18", "工商来源集体所有制");
        dictionary.put("19", "工商来源全民所有制");
        dictionary.put("20", "工商来源联营企业");
        dictionary.put("21", "工商来源股份制/股份合作制");
        dictionary.put("22", "集体经济组织");
    }

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

    public Tuple5<String, String, String, String, String> getEquityInfo(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return Tuple5.of(null, null, null, null, null);
        }
        String sql = new SQL()
                .SELECT("reg_capital_amount", "reg_capital_currency", "actual_capital_amount", "actual_capital_currency", "reg_status")
                .FROM("company_clean_info")
                .WHERE("is_deleted = 0")
                .WHERE("id = " + SqlUtils.formatValue(companyCid))
                .toString();
        Tuple5<String, String, String, String, String> res = prism116.queryForObject(sql, rs -> {
            String reg_capital_amount = rs.getString(1);
            String reg_capital_currency = rs.getString(2);
            String actual_capital_amount = rs.getString(3);
            String actual_capital_currency = rs.getString(4);
            String reg_status = rs.getString(5);
            // 清洗
            if (StringUtils.isNumeric(reg_capital_amount) && !"0".equals(reg_capital_amount)) {
                reg_capital_currency = TycUtils.isValidName(reg_capital_currency) ? reg_capital_currency : "人民币";
            } else {
                reg_capital_amount = null;
                reg_capital_currency = null;
            }
            if (StringUtils.isNumeric(actual_capital_amount) && !"0".equals(actual_capital_amount)) {
                actual_capital_currency = TycUtils.isValidName(actual_capital_currency) ? actual_capital_currency : "人民币";
            } else {
                actual_capital_amount = null;
                actual_capital_currency = null;
            }
            reg_status = TycUtils.isValidName(reg_status) ? reg_status : null;
            return Tuple5.of(reg_capital_amount, reg_capital_currency, actual_capital_amount, actual_capital_currency, reg_status);
        });
        return res != null ? res : Tuple5.of(null, null, null, null, null);
    }

    public Tuple2<String, String> getProperty(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return Tuple2.of("0", dictionary.get("0"));
        }
        String sql = new SQL()
                .SELECT("entity_property")
                .FROM("tyc_entity_general_property_reference")
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        String res = companyBase465.queryForObject(sql, rs -> rs.getString(1));
        String prop = dictionary.get(res);
        return prop != null ? Tuple2.of(res, prop) : Tuple2.of("0", dictionary.get("0"));
    }

    public Map<String, Object> getPropertyRef(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return new HashMap<>();
        }
        String sql = new SQL()
                .SELECT("*")
                .FROM("tyc_entity_general_property_reference")
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        List<Map<String, Object>> columnMaps = companyBase465.queryForColumnMaps(sql);
        if (columnMaps.isEmpty()) {
            return new HashMap<>();
        }
        return columnMaps.get(0);
    }

    public String getTax(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return null;
        }
        String sql = new SQL()
                .SELECT("if(property1 is not null and property1 <> '', property1, property4)")
                .FROM("company")
                .WHERE("id = " + SqlUtils.formatValue(companyCid))
                .toString();
        return prism116.queryForObject(sql, rs -> rs.getString(1));
    }

    public Map<String, Object> queryCompanyInfo(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return new HashMap<>();
        }
        String sql = new SQL()
                .SELECT("*")
                .FROM("company")
                .WHERE("id = " + SqlUtils.formatValue(companyCid))
                .toString();
        List<Map<String, Object>> columnMaps = prism116.queryForColumnMaps(sql);
        if (columnMaps.isEmpty()) {
            return new HashMap<>();
        }
        return columnMaps.get(0);
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

    public Map<String, Object> queryOrgInfo(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return new HashMap<>();
        }
        String sql = new SQL()
                .SELECT("*")
                .FROM("organization_info")
                .WHERE("deleted = 0")
                .WHERE("company_id = " + SqlUtils.formatValue(companyCid))
                .toString();
        List<Map<String, Object>> columnMaps = prism116.queryForColumnMaps(sql);
        if (columnMaps.isEmpty()) {
            return new HashMap<>();
        }
        return columnMaps.get(0);
    }

    public String gid2Cid(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return "0";
        }
        String sql = new SQL().SELECT("id")
                .FROM("enterprise")
                .WHERE("deleted = 0")
                .WHERE("graph_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        String res = prism464.queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "0";
    }
}
