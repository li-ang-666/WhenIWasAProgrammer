package com.liang.flink.project.company.base.info;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompanyBaseInfoService {
    private final CompanyBaseInfoDao dao = new CompanyBaseInfoDao();

    public List<String> invoke(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return new ArrayList<>();
        }
        ArrayList<String> sqls = new ArrayList<>();
        String deleteSql1 = new SQL()
                .DELETE_FROM("entity_mainland_general_registration_info_details")
                .WHERE("id = " + companyCid)
                .toString();
        String deleteSql2 = new SQL()
                .DELETE_FROM("entity_mainland_public_institution_registration_info_details")
                .WHERE("id = " + companyCid)
                .toString();
        sqls.add(deleteSql1);
        sqls.add(deleteSql2);
        Map<String, Object> enterpriseMap = dao.queryEnterprise(companyCid);
        if (enterpriseMap.isEmpty()) {
            return sqls;
        }
        String sourceFlag = String.valueOf(enterpriseMap.get("source_flag"));
        if (sourceFlag.matches("http://qyxy.baic.gov.cn/(_\\d+)?")) {
            sqls.add(getCompanySql(enterpriseMap));
        } else if (sourceFlag.equals("institution")) {
            String sql = getInstitutionSql(enterpriseMap);
            if (sql != null) {
                sqls.add(sql);
            }
        }
        return sqls;
    }

    private String getCompanySql(Map<String, Object> enterpriseMap) {
        String companyCid = String.valueOf(enterpriseMap.get("id"));
        String companyGid = String.valueOf(enterpriseMap.get("graph_id"));
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("id", companyCid);
        columnMap.put("tyc_unique_entity_id", companyGid);
        columnMap.put("entity_name_valid", String.valueOf(enterpriseMap.get("name")));
        Tuple4<String, String, String, String> equityInfo = dao.getEquityInfo(companyCid);
        columnMap.put("register_capital_amount", equityInfo.f0);
        columnMap.put("register_capital_currency", equityInfo.f1);
        columnMap.put("actual_capital_amount", equityInfo.f2);
        columnMap.put("actual_capital_currency", equityInfo.f3);
        // 登记经营状态
        columnMap.put("entity_registration_status", ifNull(enterpriseMap, "reg_status", ""));
        // 工商注册号
        columnMap.put("register_number", ifNull(enterpriseMap, "reg_number", ""));
        // 统一社会信用代码
        columnMap.put("unified_social_credit_code", ifNull(enterpriseMap, "code", ""));
        // 实体性质
        columnMap.put("entity_property", dao.getProperty(companyGid));
        // 实体性质原始(企业类型)
        columnMap.put("entity_property_original", ifNull(enterpriseMap, "company_org_type", 0));
        // 登记注册地址
        columnMap.put("entity_register_address", ifNull(enterpriseMap, "reg_location", ""));
        // 成立日期
        columnMap.put("registration_date", ifNull(enterpriseMap, "establish_date", null));
        // 经营期限
        columnMap.put("business_term_start_date", ifNull(enterpriseMap, "from_date", null));
        columnMap.put("business_term_end_date", ifNull(enterpriseMap, "to_date", null));
        columnMap.put("business_term_is_permanent", "-1");
        // 经营范围
        columnMap.put("business_registration_scope", ifNull(enterpriseMap, "business_scope", ""));
        // 登记机关
        columnMap.put("registration_institute", ifNull(enterpriseMap, "reg_institute", ""));
        // 核准日期
        columnMap.put("approval_date", ifNull(enterpriseMap, "approved_date", null));
        // 组织机构代码
        columnMap.put("organization_code", ifNull(enterpriseMap, "org_number", ""));
        // 纳税人识别号
        columnMap.put("taxpayer_identification_code", dao.getTax(companyCid));
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        return new SQL().INSERT_INTO("entity_mainland_general_registration_info_details")
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
    }

    private String getInstitutionSql(Map<String, Object> enterpriseMap) {
        String companyCid = String.valueOf(enterpriseMap.get("id"));
        String companyGid = String.valueOf(enterpriseMap.get("graph_id"));
        Map<String, Object> govMap = dao.queryGovInfo(companyCid);
        if (govMap.isEmpty()) {
            return null;
        }
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("id", companyCid);
        columnMap.put("tyc_unique_entity_id", companyGid);
        columnMap.put("entity_name_valid", String.valueOf(enterpriseMap.get("name")));
        Tuple4<String, String, String, String> equityInfo = dao.getEquityInfo(companyCid);
        columnMap.put("register_capital_amount", equityInfo.f0);
        columnMap.put("register_capital_currency", equityInfo.f1);
        // 登记经营状态
        columnMap.put("entity_registration_status", ifNull(enterpriseMap, "reg_status", ""));
        // 举办单位名称
        columnMap.put("register_unit_public_institution", ifNull(govMap, "reg_unit_name", ""));
        // 经费来源
        columnMap.put("public_institution_funding_source", ifNull(govMap, "expend_source", ""));
        // 登记机关
        columnMap.put("registration_institute", ifNull(enterpriseMap, "reg_institute", ""));
        // 原证书号
        columnMap.put("original_certificate_number_public_institution", String.valueOf(govMap.get("old_cert")).replaceAll("[^0-9]", ""));
        // 工商注册号
        columnMap.put("register_number", ifNull(enterpriseMap, "reg_number", ""));
        // 统一社会信用代码
        columnMap.put("unified_social_credit_code", ifNull(enterpriseMap, "code", ""));
        // 经营期限
        columnMap.put("business_term_start_date", ifNull(enterpriseMap, "from_date", null));
        columnMap.put("business_term_end_date", ifNull(enterpriseMap, "to_date", null));
        columnMap.put("business_term_is_permanent", "-1");
        // 登记注册地址
        columnMap.put("entity_register_address", ifNull(enterpriseMap, "reg_location", ""));
        // 经营范围
        columnMap.put("business_registration_scope", ifNull(enterpriseMap, "business_scope", ""));
        // 是否中央级事业单位
        columnMap.put("is_national_public_institution", "4".equals(dao.getProperty(companyGid)));
        // 组织机构代码
        columnMap.put("organization_code", ifNull(enterpriseMap, "org_number", ""));
        // 纳税人识别号
        columnMap.put("taxpayer_identification_code", dao.getTax(companyCid));
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        return new SQL().INSERT_INTO("entity_mainland_public_institution_registration_info_details")
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
    }

    private Object ifNull(Map<String, Object> map, String key, Object defaultValue) {
        Object value = map.get(key);
        return value != null ? value : defaultValue;
    }
}
