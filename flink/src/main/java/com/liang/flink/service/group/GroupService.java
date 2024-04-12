package com.liang.flink.service.group;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class GroupService {
    private static final long REGISTER_CAPITAL_AMT = 100 * 10_000_000L;
    private static final List<String> ENTITY_PROPERTY_BLACK_LIST = Arrays.asList("11", "17");
    private final GroupDao dao = new GroupDao();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        List<Map<String, Object>> columnMaps = new GroupService().tryCreateGroup("3069334211");
        for (Map<String, Object> columnMap : columnMaps) {
            System.out.println(columnMap);
        }
    }

    public List<Map<String, Object>> tryCreateGroup(String companyId) {
        List<Map<String, Object>> result = new ArrayList<>();
        // 合法公司
        if (!TycUtils.isUnsignedId(companyId)) {
            return result;
        }
        Map<String, Object> companyIndexMap = dao.queryCompanyIndex(companyId);
        if (companyIndexMap.isEmpty()) {
            return result;
        }
        // 排除 个体工商户, 个人独资企业
        if (ENTITY_PROPERTY_BLACK_LIST.contains(companyId)) {
            return result;
        }
        // 排除分支机构
        if (dao.isCompanyBranch(companyId)) {
            return result;
        }
        // 查询第一股比的所有公司股东
        List<String> maxRatioShareholderIds = dao.queryMaxRatioShareholder(companyId);
        if (maxRatioShareholderIds.isEmpty()) {
            return result;
        }
        // 按照 注册资本desc, 集团规模desc, 成立时间asc 对股东排序
        PriorityQueue<ComparableShareholder> comparableShareholders = new PriorityQueue<>();
        for (String maxRatioShareholderId : maxRatioShareholderIds) {
            Map<String, Object> shareholderCompanyIndexMap = dao.queryCompanyIndex(maxRatioShareholderId);
            // 合法股东
            if (shareholderCompanyIndexMap.isEmpty()) {
                continue;
            }
            // 注册资本
            long shareholderRegisterCapitalAmt = Long.parseLong(String.valueOf(shareholderCompanyIndexMap.get("register_capital_amt")));
            if (shareholderRegisterCapitalAmt < REGISTER_CAPITAL_AMT) {
                continue;
            }
            String dirtyEstablishDate = String.valueOf(shareholderCompanyIndexMap.get("establish_date"));
            String establishDate = TycUtils.isValidName(dirtyEstablishDate) ? dirtyEstablishDate : "9999-12-31 00:00:00";
            ComparableShareholder comparableShareholder = ComparableShareholder.builder()
                    .id(String.valueOf(shareholderCompanyIndexMap.get("company_id")))
                    .name(String.valueOf(shareholderCompanyIndexMap.get("company_name")))
                    .groupSize(dao.queryGroupSize(maxRatioShareholderId))
                    .registerCapitalAmt(shareholderRegisterCapitalAmt)
                    .establishDate(establishDate)
                    .build();
            comparableShareholders.add(comparableShareholder);
        }
        if (comparableShareholders.isEmpty()) {
            return result;
        }
        ComparableShareholder finalOnlyShareholder = comparableShareholders.poll();
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("group_id", finalOnlyShareholder.getId());
        columnMap.put("group_name", finalOnlyShareholder.getName());
        columnMap.put("id", companyId);
        columnMap.put("company_id", companyId);
        columnMap.put("company_name", String.valueOf(companyIndexMap.get("company_name")));
        result.add(columnMap);
        return result;
    }
}
