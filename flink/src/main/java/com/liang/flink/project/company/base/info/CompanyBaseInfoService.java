package com.liang.flink.project.company.base.info;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class CompanyBaseInfoService {
    private final static String COMPANY = "entity_mainland_general_registration_info_details";
    private final static String INSTITUTION = "entity_mainland_public_institution_registration_info_details";
    private final CompanyBaseInfoDao dao = new CompanyBaseInfoDao();

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
        Map<String, Object> enterpriseMap = dao.queryEnterprise(companyCid);
        // 查询enterprise, 若缺失, 双删
        if (enterpriseMap.isEmpty()) {
            log.warn("company_cid {} 在 enterprise 缺失", companyCid);
            sqls.add(deleteSql1);
            sqls.add(deleteSql2);
            return sqls;
        }
        // 分发两种处理逻辑 or 双删
        String sourceFlag = String.valueOf(enterpriseMap.get("source_flag"));
        if (sourceFlag.matches("http://qyxy.baic.gov.cn/(_\\d+)?")) {
            String sql = getCompanySql(enterpriseMap);
            sqls.add(sql != null ? sql : deleteSql1);
        } else if (sourceFlag.equals("institution")) {
            String sql = getInstitutionSql(enterpriseMap);
            sqls.add(sql != null ? sql : deleteSql2);
        } else {
        }
        return sqls;
    }

    private String getCompanySql(Map<String, Object> enterpriseMap) {
        String companyCid = String.valueOf(enterpriseMap.get("id"));
        String companyGid = String.valueOf(enterpriseMap.get("graph_id"));
        String entityProperty = dao.getProperty(companyGid);
        if (!entityProperty.startsWith("工商来源") && !entityProperty.equals("农民专业合作社")) {
            log.warn("company_cid {}, company_gid {} 是[{}], 不是工商", companyCid, companyGid, entityProperty);
            return null;
        }
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
        columnMap.put("entity_property", entityProperty);
        // 实体性质原始(企业类型)
        columnMap.put("entity_property_original", ifNull(enterpriseMap, "company_org_type", ""));
        // 登记注册地址
        columnMap.put("entity_register_address", ifNull(enterpriseMap, "reg_location", ""));
        // 成立日期
        columnMap.put("registration_date", ifNull(enterpriseMap, "establish_date", null));
        // 经营期限
        columnMap.put("business_term_start_date", ifNull(enterpriseMap, "from_date", null));
        columnMap.put("business_term_end_date", ifNull(enterpriseMap, "to_date", null));
        columnMap.put("business_term_is_permanent", enterpriseMap.get("to_date") == null);
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
        String entityProperty = dao.getProperty(companyGid);
        if (!entityProperty.endsWith("事业单位")) {
            log.warn("company_cid {}, company_gid {} 是[{}], 不是事业单位", companyCid, companyGid, entityProperty);
            return null;
        }
        Map<String, Object> govMap = dao.queryGovInfo(companyCid);
        if (govMap.isEmpty()) {
            log.warn("company_cid {}, company_gid {} 在 gov_unit 缺失", companyCid, companyGid);
            return null;
        }
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("id", companyCid);
        columnMap.put("tyc_unique_entity_id", companyGid);
        columnMap.put("entity_name_valid", String.valueOf(enterpriseMap.get("name")));
        Tuple4<String, String, String, String> equityInfo = dao.getEquityInfo(companyCid);
        columnMap.put("register_capital_amount", equityInfo.f0);
        columnMap.put("register_capital_currency", "人民币");
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
        // 工商注册号 基础数据:端上无
        columnMap.put("register_number", ifNull(enterpriseMap, "reg_number", ""));
        // 统一社会信用代码
        columnMap.put("unified_social_credit_code", ifNull(enterpriseMap, "code", ""));
        // 经营期限
        Tuple2<String, String> validTime = getStartAndEndDate(String.valueOf(govMap.get("valid_time")));
        columnMap.put("business_term_start_date", validTime.f0);
        columnMap.put("business_term_end_date", validTime.f1);
        columnMap.put("business_term_is_permanent", validTime.f1 == null);
        // 登记注册地址
        columnMap.put("entity_register_address", ifNull(enterpriseMap, "reg_location", ""));
        // 经营范围
        columnMap.put("business_registration_scope", ifNull(enterpriseMap, "business_scope", ""));
        // 是否中央级事业单位
        columnMap.put("is_national_public_institution", "中央级事业单位".equals(entityProperty));
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

    private Tuple2<String, String> getStartAndEndDate(String text) {
        if (text != null && text.matches(".*?(\\d{4}).*?(\\d{2}).*?(\\d{2}).*?(\\d{4}).*?(\\d{2}).*?(\\d{2}).*")) {
            return Tuple2.of(
                    text.replaceAll(".*?(\\d{4}).*?(\\d{2}).*?(\\d{2}).*?(\\d{4}).*?(\\d{2}).*?(\\d{2}).*", "$1-$2-$3"),
                    text.replaceAll(".*?(\\d{4}).*?(\\d{2}).*?(\\d{2}).*?(\\d{4}).*?(\\d{2}).*?(\\d{2}).*", "$4-$5-$6"));
        } else {
            return Tuple2.of(null, null);
        }
    }
}
