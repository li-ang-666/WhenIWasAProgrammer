package com.liang.flink.project.company.base.info;

import com.liang.common.service.SQL;
import com.liang.common.util.DateTimeUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class CompanyBaseInfoService {
    private final static String COMPANY = "entity_mainland_general_registration_info_details";
    private final static String INSTITUTION = "entity_mainland_public_institution_registration_info_details";
    private final CompanyBaseInfoDao dao = new CompanyBaseInfoDao();

    private static Tuple3<String, String, Boolean> getTimeInfo(String text) {
        // 非日期内容
        if (text.contains("无固定期限") || text.contains("长期") || text.contains("永久")) {
            return Tuple3.of(null, null, true);
        }
        if (text.contains("证书已公告废止") || text.contains("未公示")) {
            return Tuple3.of(null, null, false);
        }
        // 内容格式化, 提取日期
        text = text.replaceAll("自|从|\\s|日", "").replaceAll("到", "至").replaceAll("[年月]", "-");
        // 防止数组空指针, 补全
        if (!text.contains("至")) {
            text = text + "至";
        }
        String[] split = (" " + text + " ").split("至");
        String start = split[0].trim().matches("\\d{4}-\\d{2}-\\d{2}") ? split[0].trim() : null;
        String end = split[1].trim().matches("\\d{4}-\\d{2}-\\d{2}") ? split[1].trim() : null;
        // 开始日期 大于 结束日期, 脏数据, 跳出
        if (start != null && end != null && start.compareTo(end) > 0) {
            return Tuple3.of(null, null, false);
        }
        // 开始日期 大于 今天 or 开始日期 小于 1900-01-01, 脏数据, 跳出
        if (start != null && (start.compareTo(DateTimeUtils.currentDate()) > 0 || start.compareTo("1900-01-01") < 0)) {
            return Tuple3.of(null, null, false);
        }
        // 结束日期 大于 2099-12-31 or 结束日期 小于 1900-01-01, 脏数据, 跳出
        if (end != null && (end.compareTo("2099-12-31") > 0 || end.compareTo("1900-01-01") < 0)) {
            return Tuple3.of(null, null, false);
        }
        // 特殊判断, 2099-12-31结束, 代表长期
        if ("2099-12-31".equals(end)) {
            return Tuple3.of(start, end, true);
        }
        // 有开始无结束, 代表长期
        if (start != null && end == null) {
            return Tuple3.of(start, end, true);
        }
        return Tuple3.of(start, end, false);
    }

    public static void main(String[] args) {
        System.out.println(getTimeInfo("长期"));
        System.out.println(getTimeInfo("证书已公告废止"));
        System.out.println(getTimeInfo("至无固定期限"));
        System.out.println(getTimeInfo("至2019-03-31"));
        System.out.println(getTimeInfo("至"));
        System.out.println(getTimeInfo("自至2032年03月31日"));
        System.out.println(getTimeInfo("自至"));
        System.out.println(getTimeInfo("自2023年09月05日至2028年09月04日"));
        System.out.println(getTimeInfo("自2023年09月05日到2028年09月04日"));
        System.out.println(getTimeInfo("自2020-03-23至2021-03-31"));
        System.out.println(getTimeInfo("2022-05-23至2027-05-22"));
        System.out.println(getTimeInfo("2017年01月16日至2022年01月16日"));
        System.out.println(getTimeInfo("2014-11-20至"));
        System.out.println(getTimeInfo("- 至 2025-09-02"));
        System.out.println(getTimeInfo("1905-06-18 至 -"));
        System.out.println(getTimeInfo("2018-05-22 至 2022-08-27"));
        System.out.println(getTimeInfo("2099-12-31 至 2099-12-31"));
    }

    public List<String> invoke(String companyCid) {
        if (!TycUtils.isUnsignedId(companyCid)) {
            return new ArrayList<>();
        }
        String deleteSql1 = new SQL()
                .DELETE_FROM(COMPANY)
                .WHERE("id = " + companyCid)
                .toString();
        String deleteSql2 = new SQL()
                .DELETE_FROM(INSTITUTION)
                .WHERE("id = " + companyCid)
                .toString();
        ArrayList<String> sqls = new ArrayList<>();
        // 查询enterprise, 若缺失, 双删
        Map<String, Object> enterpriseMap = dao.queryEnterprise(companyCid);
        if (enterpriseMap.isEmpty()) {
            sqls.add(deleteSql1);
            sqls.add(deleteSql2);
            return sqls;
        }
        // 查询企业性质
        String companyGid = String.valueOf(enterpriseMap.get("graph_id"));
        Tuple2<String, String> entityPropertyTp2 = dao.getProperty(companyGid);
        String entityPropertyId = entityPropertyTp2.f0;
        String entityPropertyName = entityPropertyTp2.f1;
        enterpriseMap.put("entity_property", entityPropertyId);
        // 分发处理
        String sourceFlag = String.valueOf(enterpriseMap.get("source_flag"));
        if (entityPropertyName.startsWith("工商来源") || entityPropertyName.equals("农民专业合作社")) {
            sqls.add(getCompanySql(enterpriseMap));
        } else if (entityPropertyName.endsWith("事业单位") && "institution".equals(sourceFlag)) {
            String sql = getInstitutionSql(enterpriseMap);
            sqls.add(sql != null ? sql : deleteSql2);
        } else if (entityPropertyName.endsWith("事业单位") && sourceFlag.startsWith("org_19")) {
            String sql = getOrgSql(enterpriseMap);
            sqls.add(sql != null ? sql : deleteSql2);
        } else {
            sqls.add(deleteSql1);
            sqls.add(deleteSql2);
        }
        return sqls;
    }

    private String getCompanySql(Map<String, Object> enterpriseMap) {
        String companyCid = String.valueOf(enterpriseMap.get("id"));
        String companyGid = String.valueOf(enterpriseMap.get("graph_id"));
        String entityProperty = String.valueOf(enterpriseMap.get("entity_property"));
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("id", companyCid);
        columnMap.put("tyc_unique_entity_id", companyGid);
        columnMap.put("entity_name_valid", String.valueOf(enterpriseMap.get("name")));
        Tuple5<String, String, String, String, String> equityInfo = dao.getEquityInfo(companyCid);
        columnMap.put("register_capital_amount", StringUtils.isNumeric(equityInfo.f0) ? equityInfo.f0 : null);
        columnMap.put("register_capital_currency", equityInfo.f1);
        columnMap.put("actual_capital_amount", StringUtils.isNumeric(equityInfo.f2) ? equityInfo.f2 : null);
        columnMap.put("actual_capital_currency", equityInfo.f3);
        // 登记经营状态
        columnMap.put("entity_registration_status", equityInfo.f4);
        // 工商注册号
        columnMap.put("register_number", ifNull(enterpriseMap, "reg_number", ""));
        // 统一社会信用代码
        columnMap.put("unified_social_credit_code", ifNull(enterpriseMap, "code", ""));
        // 实体性质
        columnMap.put("entity_property", entityProperty);
        // 实体性质原始(企业类型)
        columnMap.put("entity_property_original", ifNull(enterpriseMap, "company_org_type", ""));
        // 登记注册地址
        columnMap.put("entity_register_address", ifNull(enterpriseMap, "reg_location", ""));
        // 成立日期
        columnMap.put("registration_date", ifNull(enterpriseMap, "establish_date", null));
        // 经营期限
        String text = enterpriseMap.get("from_date") + "至" + enterpriseMap.get("to_date");
        Tuple3<String, String, Boolean> timeInfo = getTimeInfo(text);
        columnMap.put("business_term_start_date", timeInfo.f0 != null ? timeInfo.f0 : null);
        columnMap.put("business_term_end_date", timeInfo.f1 != null ? timeInfo.f1 : null);
        columnMap.put("business_term_is_permanent", timeInfo.f2);
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
        return new SQL().REPLACE_INTO(COMPANY)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
    }

    private String getInstitutionSql(Map<String, Object> enterpriseMap) {
        String companyCid = String.valueOf(enterpriseMap.get("id"));
        String companyGid = String.valueOf(enterpriseMap.get("graph_id"));
        String entityProperty = String.valueOf(enterpriseMap.get("entity_property"));
        Map<String, Object> govMap = dao.queryGovInfo(companyCid);
        if (govMap.isEmpty()) {
            return null;
        }
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("id", companyCid);
        columnMap.put("tyc_unique_entity_id", companyGid);
        columnMap.put("entity_name_valid", String.valueOf(enterpriseMap.get("name")));
        Tuple5<String, String, String, String, String> equityInfo = dao.getEquityInfo(companyCid);
        columnMap.put("register_capital_amount", StringUtils.isNumeric(equityInfo.f0) ? equityInfo.f0 : null);
        columnMap.put("register_capital_currency", equityInfo.f1);
        // 登记经营状态
        columnMap.put("entity_registration_status", equityInfo.f4);
        // 举办单位名称
        columnMap.put("register_unit_public_institution", ifNull(govMap, "reg_unit_name", ""));
        // 经费来源
        columnMap.put("public_institution_funding_source", ifNull(govMap, "expend_source", ""));
        // 登记机关
        columnMap.put("registration_institute", ifNull(govMap, "hold_unit", ""));
        // 原证书号
        columnMap.put("original_certificate_number_public_institution", String.valueOf(govMap.get("old_cert")).replaceAll("[^0-9]", ""));
        // 工商注册号 基础数据:端上无
        columnMap.put("register_number", ifNull(enterpriseMap, "reg_number", ""));
        // 统一社会信用代码
        columnMap.put("unified_social_credit_code", ifNull(govMap, "us_credit_code", ""));
        // 经营期限
        Tuple3<String, String, Boolean> timeInfo = getTimeInfo(String.valueOf(govMap.get("valid_time")));
        columnMap.put("business_term_start_date", timeInfo.f0 != null ? timeInfo.f0 : null);
        columnMap.put("business_term_end_date", timeInfo.f1 != null ? timeInfo.f1 : null);
        columnMap.put("business_term_is_permanent", timeInfo.f2);
        // 登记注册地址
        columnMap.put("entity_register_address", ifNull(govMap, "address", ""));
        // 经营范围
        columnMap.put("business_registration_scope", ifNull(govMap, "scope", ""));
        // 实体性质
        columnMap.put("entity_property", entityProperty);
        // 是否中央级事业单位
        columnMap.put("is_national_public_institution", "4".equals(entityProperty));
        // 组织机构代码 基础数据:端上无
        columnMap.put("organization_code", ifNull(enterpriseMap, "org_number", ""));
        // 纳税人识别号 基础数据:端上无
        columnMap.put("taxpayer_identification_code", dao.getTax(companyCid));
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        return new SQL().REPLACE_INTO(INSTITUTION)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
    }

    private String getOrgSql(Map<String, Object> enterpriseMap) {
        String companyCid = String.valueOf(enterpriseMap.get("id"));
        String companyGid = String.valueOf(enterpriseMap.get("graph_id"));
        String entityProperty = String.valueOf(enterpriseMap.get("entity_property"));
        Map<String, Object> orgMap = dao.queryOrgInfo(companyCid);
        if (orgMap.isEmpty()) {
            return null;
        }
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("id", companyCid);
        columnMap.put("tyc_unique_entity_id", companyGid);
        columnMap.put("entity_name_valid", String.valueOf(enterpriseMap.get("name")));
        Tuple5<String, String, String, String, String> equityInfo = dao.getEquityInfo(companyCid);
        columnMap.put("register_capital_amount", equityInfo.f0);
        columnMap.put("register_capital_currency", "人民币");
        // 登记经营状态
        columnMap.put("entity_registration_status", equityInfo.f4);
        // 举办单位名称 ~
        // 经费来源 ~
        // 登记机关
        columnMap.put("registration_institute", ifNull(orgMap, "registration_authority", ""));
        // 原证书号 ~
        // 工商注册号 基础数据:端上无
        columnMap.put("register_number", ifNull(enterpriseMap, "registration_number", ""));
        // 统一社会信用代码
        columnMap.put("unified_social_credit_code", ifNull(orgMap, "unified_social_credit_code", ""));
        // 经营期限
        String expiryDate = String.valueOf(orgMap.get("expiry_date"));
        Tuple3<String, String, Boolean> timeInfo = getTimeInfo(expiryDate);
        columnMap.put("business_term_start_date", timeInfo.f0 != null ? timeInfo.f0 : null);
        columnMap.put("business_term_end_date", timeInfo.f1 != null ? timeInfo.f1 : null);
        columnMap.put("business_term_is_permanent", timeInfo.f2);
        // 登记注册地址
        columnMap.put("entity_register_address", ifNull(orgMap, "address", ""));
        // 经营范围
        columnMap.put("business_registration_scope", ifNull(orgMap, "business_scope", ""));
        // 实体性质
        columnMap.put("entity_property", entityProperty);
        // 是否中央级事业单位
        columnMap.put("is_national_public_institution", "4".equals(entityProperty));
        // 组织机构代码 基础数据:端上无
        columnMap.put("organization_code", ifNull(enterpriseMap, "org_number", ""));
        // 纳税人识别号 基础数据:端上无
        columnMap.put("taxpayer_identification_code", dao.getTax(companyCid));
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        return new SQL().REPLACE_INTO(INSTITUTION)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
    }

    private Object ifNull(Map<String, Object> map, String key, Object defaultValue) {
        Object value = map.get(key);
        return value != null ? value : defaultValue;
    }
}
