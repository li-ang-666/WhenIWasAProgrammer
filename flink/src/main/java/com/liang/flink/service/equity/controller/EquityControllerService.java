package com.liang.flink.service.equity.controller;

import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.service.equity.bfs.EquityBfsDao;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class EquityControllerService {
    private static final String THRESHOLD_PERCENT_THIRTY = "0.300000";
    private static final String THRESHOLD_PERCENT_FIFTY = "0.500000";
    private static final Set<String> USCC_TWO_WHITE_LIST = new HashSet<>(Arrays.asList("31", "91", "92", "93"));
    private final EquityControllerDao controllerDao = new EquityControllerDao();
    private final EquityBfsDao bfsDao = new EquityBfsDao();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        for (Map<String, Object> columnMap : new EquityControllerService().processController("14427175")) {
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
        System.out.println(StrUtil.repeat("=", 10));
        for (Map<String, Object> columnMap : new EquityControllerService().processController("2318455639")) {
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
        System.out.println(StrUtil.repeat("=", 10));
        for (Map<String, Object> columnMap : new EquityControllerService().processController("1516070")) {
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
    }

    public List<Map<String, Object>> processController(String companyId) {
        List<Map<String, Object>> columnMaps = new ArrayList<>();
        // 验证id
        if (!TycUtils.isUnsignedId(companyId)) return columnMaps;
        // 查询company_index
        Map<String, Object> companyInfo = controllerDao.queryCompanyInfo(companyId);
        // 验证company_index
        if (companyInfo.isEmpty()) return columnMaps;
        // 验证名称
        String companyName = String.valueOf(companyInfo.get("company_name"));
        if (!TycUtils.isValidName(companyName)) return columnMaps;
        // 企业类型屏蔽逻辑
        String usccPrefixTwo = (companyInfo.get("unified_social_credit_code") + "suffix to prevent OutOfBoundsException").substring(0, 2);
        if (!USCC_TWO_WHITE_LIST.contains(usccPrefixTwo)) return columnMaps;
        // 存在上市公告的实际控制人
        List<Map<String, Object>> listedAnnouncedControllers = controllerDao.queryListedAnnouncedControllers(companyId);
        // 如果查询到了数据, 那一定会return, 即使是空return
        if (!listedAnnouncedControllers.isEmpty()) {
            for (Map<String, Object> listedAnnouncedController : listedAnnouncedControllers) {
                String listedAnnouncedControllerId = String.valueOf(listedAnnouncedController.get("id"));
                String holdingRatio = String.valueOf(listedAnnouncedController.get("holding_ratio"));
                // 验证id
                if (!TycUtils.isTycUniqueEntityId(listedAnnouncedControllerId)) continue;
                // 验证ratio
                String ratio;
                try {
                    ratio = new BigDecimal(holdingRatio).setScale(6, RoundingMode.DOWN).toPlainString();
                } catch (Exception ignore) {
                    continue;
                }
                // 查询股东维表
                Map<String, Object> shareholderInfo = bfsDao.queryHumanOrCompanyInfo(listedAnnouncedControllerId);
                // 验证股东维表
                if (isInvalidShareholder(shareholderInfo)) continue;
                // 构造columnMap
                columnMaps.add(getSpecialColumnMap(companyId, companyName, shareholderInfo, ratio));
            }
            return columnMaps;
        }
        // 股权穿透
        List<Map<String, Object>> ratioPathCompanyMaps = controllerDao.queryRatioPathCompany(companyId);
        // 遍历股东, 按照ratio做group by
        TreeMap<String, List<Map<String, Object>>> ratioToShareholdersWithSameRatio = new TreeMap<>();
        for (Map<String, Object> shareholder : ratioPathCompanyMaps) {
            String ratio = String.valueOf(shareholder.get("investment_ratio_total"));
            ratioToShareholdersWithSameRatio.compute(ratio, (k, v) -> {
                List<Map<String, Object>> shareholdersWithSameRatio = (v != null) ? v : new ArrayList<>();
                shareholdersWithSameRatio.add(shareholder);
                return shareholdersWithSameRatio;
            });
        }
        // 最大比例及该比例下的股东
        Map.Entry<String, List<Map<String, Object>>> maxRatioToShareholders = ratioToShareholdersWithSameRatio.lastEntry();
        String maxRatio = maxRatioToShareholders.getKey();
        List<Map<String, Object>> maxRatioShareholders = maxRatioToShareholders.getValue();
        // 预测总持股比例>=50%且预测总持股比例>=50%的股东数量=1的
        if (maxRatio.compareTo(THRESHOLD_PERCENT_FIFTY) >= 0 && maxRatioShareholders.size() == 1) {
            // 无论该最终股东是否为自然人, 均可为实际控制人
            columnMaps.add(getColumnMap(companyId, companyName, maxRatioShareholders.get(0)));
            return columnMaps;
        }
        // 存在预测总持股比例>=30%的股东
        else if (maxRatio.compareTo(THRESHOLD_PERCENT_THIRTY) >= 0) {
            for (Map<String, Object> maxRatioShareholder : maxRatioShareholders) {
                String shareholderType = String.valueOf(maxRatioShareholder.get("shareholder_entity_type"));
                String shareholderId = String.valueOf(maxRatioShareholder.get("shareholder_id"));
                // 非自然人, 则直接为实际控制人
                if (shareholderType.equals("1")) {
                    columnMaps.add(getColumnMap(companyId, companyName, maxRatioShareholder));
                }
                // 自然人, 判断该自然人是否在当前企业担任 董事长、执行董事 职位
                else if (shareholderType.equals("2") && controllerDao.queryIsPersonnel(companyId, shareholderId)) {
                    columnMaps.add(getColumnMap(companyId, companyName, maxRatioShareholder));
                }
            }
            return columnMaps;
        }
        // 选用董事长 or 执行事务合伙人 为 实控人
        else {
            List<Map<String, Object>> vips = controllerDao.isPartnership(companyId) ? controllerDao.queryLegals(companyId) : controllerDao.queryAllPersonnels(companyId);
            for (Map<String, Object> vip : vips) {
                String vipId = String.valueOf(vip.get("id"));
                // 验证id
                if (!TycUtils.isTycUniqueEntityId(vipId)) continue;
                // 查询股东维表
                Map<String, Object> shareholderInfo = bfsDao.queryHumanOrCompanyInfo(vipId);
                // 验证股东维表
                if (isInvalidShareholder(shareholderInfo)) continue;
                // ratio
                String ratio = ratioPathCompanyMaps.stream()
                        .filter(e -> String.valueOf(e.get("shareholder_id")).equals(vipId))
                        .map(e -> String.valueOf(e.get("investment_ratio_total")))
                        .findAny()
                        .orElse("0");
                // 构造columnMap
                columnMaps.add(getSpecialColumnMap(companyId, companyName, shareholderInfo, ratio));
            }
            return columnMaps;
        }
    }

    private boolean isInvalidShareholder(Map<String, Object> shareholderInfo) {
        return
                // 股东id
                !TycUtils.isTycUniqueEntityId(String.valueOf(shareholderInfo.get("id"))) ||
                        // 股东名称
                        !TycUtils.isValidName(String.valueOf(shareholderInfo.get("name"))) ||
                        // 股东内链name_id
                        !TycUtils.isUnsignedId(String.valueOf(shareholderInfo.get("name_id"))) ||
                        // 股东内链company_id
                        !TycUtils.isUnsignedId(String.valueOf(shareholderInfo.get("company_id")));
    }

    private Map<String, Object> getColumnMap(String companyId, String companyName, Map<String, Object> ratioPathCompanyMap) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("tyc_unique_entity_id", String.valueOf(ratioPathCompanyMap.get("shareholder_id")));
        columnMap.put("entity_type_id", String.valueOf(ratioPathCompanyMap.get("shareholder_entity_type")));
        columnMap.put("entity_name_valid", String.valueOf(ratioPathCompanyMap.get("shareholder_name")));
        columnMap.put("company_id_controlled", companyId);
        columnMap.put("company_name_controlled", companyName);
        columnMap.put("equity_relation_path_cnt", 1);
        columnMap.put("estimated_equity_ratio_total", String.valueOf(ratioPathCompanyMap.get("investment_ratio_total")));
        columnMap.put("controlling_equity_relation_path_detail", "{}");
        columnMap.put("control_validation_time_year", "2023");
        columnMap.put("is_controller_tyc_unique_entity_id", "1");
        return columnMap;
    }

    private Map<String, Object> getSpecialColumnMap(String companyId, String companyName, Map<String, Object> shareholderInfoMap, String ratio) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("tyc_unique_entity_id", String.valueOf(shareholderInfoMap.get("id")));
        columnMap.put("entity_type_id", String.valueOf(shareholderInfoMap.get("id")).length() == 17 ? "2" : "1");
        columnMap.put("entity_name_valid", String.valueOf(shareholderInfoMap.get("name")));
        columnMap.put("company_id_controlled", companyId);
        columnMap.put("company_name_controlled", companyName);
        columnMap.put("equity_relation_path_cnt", 1);
        columnMap.put("estimated_equity_ratio_total", ratio);
        columnMap.put("controlling_equity_relation_path_detail", getJson(companyId, companyName, shareholderInfoMap, ratio));
        columnMap.put("control_validation_time_year", "2023");
        columnMap.put("is_controller_tyc_unique_entity_id", "1");
        return columnMap;
    }

    private String getJson(String companyId, String companyName, Map<String, Object> shareholderInfoMap, String ratio) {
        String shareholderId = String.valueOf(shareholderInfoMap.get("id"));
        String shareholderName = String.valueOf(shareholderInfoMap.get("name"));
        String shareholderNameId = String.valueOf(shareholderInfoMap.get("name_id"));
        String humanMasterCompanyId = String.valueOf(shareholderInfoMap.get("company_id"));
        ArrayList<List<Map<String, Object>>> pathList = new ArrayList<List<Map<String, Object>>>() {{
            add(new ArrayList<Map<String, Object>>() {{
                add(new LinkedHashMap<String, Object>() {{
                    put("id", shareholderId);
                    put("properties", new LinkedHashMap<String, Object>() {{
                        put("name", shareholderName);
                        if (shareholderId.length() == 17) {
                            put("node_type", "2");
                            put("human_name_id", shareholderNameId);
                            put("human_master_company_id", humanMasterCompanyId);
                        } else {
                            put("node_type", "1");
                            put("company_id", shareholderId);
                        }
                    }});
                }});
                add(new LinkedHashMap<String, Object>() {{
                    put("startNode", shareholderId);
                    put("endNode", companyId);
                    put("properties", new LinkedHashMap<String, Object>() {{
                        put("equity_ratio", ratio);
                    }});
                }});
                add(new LinkedHashMap<String, Object>() {{
                    put("id", companyId);
                    put("properties", new LinkedHashMap<String, Object>() {{
                        put("name", companyName);
                        put("node_type", "1");
                        put("company_id", companyId);
                    }});
                }});
            }});
        }};
        return JsonUtils.toString(pathList);
    }
}
