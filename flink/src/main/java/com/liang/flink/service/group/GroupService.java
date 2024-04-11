package com.liang.flink.service.group;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class GroupService {
    private static final long REGISTER_CAPITAL_AMT = 10_000_000L;
    // 个体工商户, 个人独资企业
    private static final List<String> ENTITY_PROPERTY_BLACK_LIST = Arrays.asList("11", "17");
    private static final int TARGET_SIZE = 5;

    private final GroupDao dao = new GroupDao();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        new GroupService().tryCreateGroup("2318455639");
    }

    public List<Map<String, Object>> tryCreateGroup(String groupId) {
        List<Map<String, Object>> result = new ArrayList<>();
        // 合法 gid
        if (!TycUtils.isUnsignedId(groupId)) {
            return result;
        }
        // 在 company_index 存在
        Map<String, Object> groupInfoMap = dao.queryCompanyIndex(groupId);
        if (groupInfoMap.isEmpty()) {
            return result;
        }
        // 判断注册资本
        String registerCapitalAmt = String.valueOf(groupInfoMap.get("register_capital_amt"));
        if (Long.parseLong(registerCapitalAmt) < REGISTER_CAPITAL_AMT) {
            return result;
        }
        // 查询所有被投资公司
        List<Map<String, Object>> investedCompanyMapList = dao.queryInvestedCompany(groupId);
        // 规模
        if (investedCompanyMapList.size() < TARGET_SIZE) {
            return result;
        }
        Map<String, Object> subCompanies = new HashMap<>();
        for (Map<String, Object> investedCompanyMap : investedCompanyMapList) {
            String companyEntityProperty = String.valueOf(investedCompanyMap.get("company_entity_property"));
            String investedCompanyId = String.valueOf(investedCompanyMap.get("company_id"));
            String investedCompanyName = String.valueOf(investedCompanyMap.get("company_name"));
            // 排除 个体工商户, 个人独资企业
            if (ENTITY_PROPERTY_BLACK_LIST.contains(companyEntityProperty)) {
                continue;
            }
            // 排除分支机构
            if (dao.isCompanyBranch(investedCompanyId)) {
                continue;
            }
            // 第一股比的股东是公司(返回list不为empty)
            List<String> maxRatioShareholderIds = dao.queryMaxRatioShareholder(investedCompanyId);
            if (maxRatioShareholderIds.isEmpty()) {
                continue;
            }
            // 第一股比的股东是且仅是当前母公司
            if (maxRatioShareholderIds.size() == 1 && maxRatioShareholderIds.get(0).equals(groupId)) {
                subCompanies.put(investedCompanyId, investedCompanyName);
                continue;
            }
            // 第一股比的股东包含当前母公司
            if (!maxRatioShareholderIds.contains(groupId)) {
                continue;
            }
            // 按照 注册资本desc, 集团规模desc, 成立时间asc 对股东排序
            PriorityQueue<ComparableShareholder> comparableShareholders = new PriorityQueue<>();
            for (String maxRatioShareholderId : maxRatioShareholderIds) {
                Map<String, Object> companyInfoMap = dao.queryCompanyIndex(maxRatioShareholderId);
                if (!companyInfoMap.isEmpty()) {
                    // 注册资本
                    Long shareholderRegisterCapitalAmt = Long.parseLong(String.valueOf(companyInfoMap.get("register_capital_amt")));
                    // 集团规模
                    Long groupSize = dao.queryGroupSize(maxRatioShareholderId);
                    // 成立时间
                    String dirtyEstablishDate = String.valueOf(companyInfoMap.get("establish_date"));
                    String establishDate = TycUtils.isValidName(dirtyEstablishDate) ? dirtyEstablishDate : "9999-12-31 00:00:00";
                    comparableShareholders.add(new ComparableShareholder(maxRatioShareholderId, shareholderRegisterCapitalAmt, groupSize, establishDate));
                }
            }
            if (comparableShareholders.isEmpty()) {
                continue;
            }
            if (comparableShareholders.peek().getCompanyId().equals(groupId)) {
                subCompanies.put(investedCompanyId, investedCompanyName);
            }
        }
        // 规模 >= 5
        if (subCompanies.size() < TARGET_SIZE) {
            return result;
        }
        for (Map.Entry<String, Object> subCompany : subCompanies.entrySet()) {
            Map<String, Object> columnMap = new HashMap<>();
            columnMap.put("group_id", groupId);
            columnMap.put("group_name", String.valueOf(groupInfoMap.get("company_name")));
            columnMap.put("company_id", subCompany.getKey());
            columnMap.put("company_name", subCompany.getValue());
            result.add(columnMap);
        }
        return result;
    }
}
