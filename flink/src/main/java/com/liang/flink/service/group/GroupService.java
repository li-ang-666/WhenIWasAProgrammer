package com.liang.flink.service.group;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class GroupService {
    // 1kw
    private static final long REGISTER_CAPITAL_AMT = 10_000_000L;
    // 个体工商户, 个人独资企业
    private static final List<String> ENTITY_PROPERTY_BLACK_LIST = Arrays.asList("11", "17");
    // 规模 5
    private static final int TARGET_SIZE = 5;
    private final GroupDao dao = new GroupDao();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        new GroupService().tryCreateGroup("6552196");
    }

    public void tryCreateGroup(String companyId) {
        // 在 company_index 存在
        if (!TycUtils.isUnsignedId(companyId)) {
            return;
        }
        Map<String, Object> companyIndexMap = dao.queryCompanyIndex(companyId);
        if (companyIndexMap.isEmpty()) {
            return;
        }
        // 注册资本 >= 1kw
        String registerCapitalAmt = String.valueOf(companyIndexMap.get("register_capital_amt"));
        if (!TycUtils.isUnsignedId(registerCapitalAmt)) {
            return;
        }
        if (Long.parseLong(registerCapitalAmt) < REGISTER_CAPITAL_AMT) {
            return;
        }
        // 查询所有被投资公司
        List<Map<String, Object>> investedCompanyMapList = dao.queryRatioPathCompanyNewByShareholder(companyId);
        // 规模 >= 5
        if (investedCompanyMapList.size() < TARGET_SIZE) {
            return;
        }
        Map<String, Object> subCompanies = new HashMap<>();
        for (Map<String, Object> investedCompanyMap : investedCompanyMapList) {
            String companyEntityProperty = String.valueOf(investedCompanyMap.get("company_entity_property"));
            String investedCompanyId = String.valueOf(investedCompanyMap.get("company_id"));
            String investedCompanyName = String.valueOf(investedCompanyMap.get("company_name"));
            String investmentRatioTotal = String.valueOf(investedCompanyMap.get("investment_ratio_total"));
            // 排除 个体工商户, 个人独资企业
            if (ENTITY_PROPERTY_BLACK_LIST.contains(companyEntityProperty)) {
                continue;
            }
            // 排除分支机构
            if (dao.isCompanyBranch(investedCompanyId)) {
                continue;
            }
            // 第一股比的股东是公司(返回list不为empty)
            List<String> maxRatioShareholderIds = dao.queryRatioPathCompanyNewByCompany(investedCompanyId);
            if (maxRatioShareholderIds.isEmpty()) {
                continue;
            }
            // 第一股比的股东是且仅是当前母公司
            if (maxRatioShareholderIds.size() == 1 && maxRatioShareholderIds.get(0).equals(companyId)) {
                subCompanies.put(investedCompanyId, investedCompanyName);
                continue;
            }
            // 第一股比的股东包含当前母公司
            if (!maxRatioShareholderIds.contains(companyId)) {
                continue;
            }
            // 按照 注册资本desc, 创建时间asc 排序
            String firstParent = dao.queryFirstParent(maxRatioShareholderIds);
            if (companyId.equals(firstParent)) {
                subCompanies.put(investedCompanyId, investedCompanyName);
            }
        }
        // 规模 >= 5
        if (subCompanies.size() < TARGET_SIZE) {
            return;
        }
        for (Map.Entry<String, Object> entry : subCompanies.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }
}
