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
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class EquityControlService {
    private static final String THRESHOLD_PERCENT_THIRTY = "0.300000";
    private static final String THRESHOLD_PERCENT_FIFTY = "0.500000";
    private static final Set<String> USCC_TWO_WHITE_LIST = new HashSet<>(Arrays.asList("31", "91", "92", "93"));
    private final EquityControlDao controllerDao = new EquityControlDao();
    private final EquityBfsDao bfsDao = new EquityBfsDao();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        for (Map<String, Object> columnMap : new EquityControlService().processControl("14427175")) {
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
        System.out.println(StrUtil.repeat("=", 10));
        for (Map<String, Object> columnMap : new EquityControlService().processControl("2318455639")) {
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
        System.out.println(StrUtil.repeat("=", 10));
        for (Map<String, Object> columnMap : new EquityControlService().processControl("1516070")) {
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
    }

    public List<Map<String, Object>> processControl(String companyId) {
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
        // 开始判断
        List<Map<String, Object>> listedAnnouncedControllerMaps = controllerDao.queryListedAnnouncedControllers(companyId);
        // 存在上市公告的实际控制人
        if (!listedAnnouncedControllerMaps.isEmpty()) {
            for (Map<String, Object> listedAnnouncedControllerMap : listedAnnouncedControllerMaps) {
                String listedAnnouncedControllerId = String.valueOf(listedAnnouncedControllerMap.get("id"));
                String holdingRatio = String.valueOf(listedAnnouncedControllerMap.get("holding_ratio"));
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
                Map<String, Object> shareholderMap = bfsDao.queryHumanOrCompanyInfo(listedAnnouncedControllerId);
                // 验证股东维表
                if (isInvalidShareholder(shareholderMap)) continue;
                // 构造columnMap
                columnMaps.add(getSpecialColumnMap(companyId, companyName, shareholderMap, ratio));
            }
        }
        // 走股权穿透
        else {
            // 查询ratio_path_company所有股东数据
            List<Map<String, Object>> ratioPathCompanyMaps = controllerDao.queryRatioPathCompany(companyId);
            // 按照ratio做group by
            TreeMap<String, List<Map<String, Object>>> ratioToRatioPathCompanyMapsWithSameRatio = new TreeMap<>();
            for (Map<String, Object> ratioPathCompanyMap : ratioPathCompanyMaps) {
                // 筛选isEnd的股东
                if ("0".equals(String.valueOf(ratioPathCompanyMap.get("is_end")))) continue;
                String ratio = String.valueOf(ratioPathCompanyMap.get("investment_ratio_total"));
                ratioToRatioPathCompanyMapsWithSameRatio.compute(ratio, (k, v) -> {
                    List<Map<String, Object>> RatioPathCompanyMapsWithSameRatio = (v != null) ? v : new ArrayList<>();
                    RatioPathCompanyMapsWithSameRatio.add(ratioPathCompanyMap);
                    return RatioPathCompanyMapsWithSameRatio;
                });
            }
            // 最大比例及该比例下的股东
            String maxRatio = ratioToRatioPathCompanyMapsWithSameRatio.lastEntry().getKey();
            List<Map<String, Object>> ratioPathCompanyMapsWithMaxRatio = ratioToRatioPathCompanyMapsWithSameRatio.lastEntry().getValue();
            // 预测总持股比例>=30% 且 最大股东有且只有一位
            if (maxRatio.compareTo(THRESHOLD_PERCENT_THIRTY) >= 0 && ratioPathCompanyMapsWithMaxRatio.size() == 1) {
                // 无论该最终股东是否为自然人, 均可为实际控制人
                columnMaps.add(getNormalColumnMap(ratioPathCompanyMapsWithMaxRatio.get(0), true));
            }
            // 预测总持股比例>=30% 且 最大股东有多位
            else if (maxRatio.compareTo(THRESHOLD_PERCENT_THIRTY) >= 0 && ratioPathCompanyMapsWithMaxRatio.size() > 1) {
                for (Map<String, Object> ratioPathCompanyMap : ratioPathCompanyMapsWithMaxRatio) {
                    String shareholderType = String.valueOf(ratioPathCompanyMap.get("shareholder_entity_type"));
                    String shareholderId = String.valueOf(ratioPathCompanyMap.get("shareholder_id"));
                    // 非自然人, 则直接为实际控制人
                    if (shareholderType.equals("1")) {
                        columnMaps.add(getNormalColumnMap(ratioPathCompanyMap, true));
                    }
                    // 自然人, 判断该自然人是否在当前企业担任 董事长、执行董事 职位
                    else if (shareholderType.equals("2") && controllerDao.queryIsPersonnel(companyId, shareholderId)) {
                        columnMaps.add(getNormalColumnMap(ratioPathCompanyMap, true));
                    }
                }
            }
            // 选用董事长 or 执行事务合伙人 为 实控人
            else {
                List<Map<String, Object>> vips = controllerDao.isPartnership(companyId) ? controllerDao.queryLegals(companyId) : controllerDao.queryAllPersonnels(companyId);
                for (Map<String, Object> vip : vips) {
                    String vipId = String.valueOf(vip.get("id"));
                    // 验证id
                    if (!TycUtils.isTycUniqueEntityId(vipId)) continue;
                    // 查询股东维表
                    Map<String, Object> shareholderMap = bfsDao.queryHumanOrCompanyInfo(vipId);
                    // 验证股东维表
                    if (isInvalidShareholder(shareholderMap)) continue;
                    // ratio
                    String ratio = ratioPathCompanyMaps.stream()
                            .filter(e -> String.valueOf(e.get("shareholder_id")).equals(vipId))
                            .map(e -> String.valueOf(e.get("investment_ratio_total")))
                            .findAny()
                            .orElse("0");
                    // 构造columnMap
                    columnMaps.add(getSpecialColumnMap(companyId, companyName, shareholderMap, ratio));
                }
            }
            // 补充实际控制权
            fillUpControlAbility(ratioPathCompanyMaps, columnMaps);
        }
        return columnMaps;
    }

    private void fillUpControlAbility(List<Map<String, Object>> ratioPathCompanyMaps, List<Map<String, Object>> controllerMaps) {
        // 实控人路径中间节点
        String controllerMapsString = controllerMaps.toString();
        Set<String> controllerSet = controllerMaps.stream().map(e -> String.valueOf(e.get("tyc_unique_entity_id"))).collect(Collectors.toSet());
        for (Map<String, Object> ratioPathCompanyMap : ratioPathCompanyMaps) {
            // 股权比例 >= 50%
            String investmentRatioTotal = String.valueOf(ratioPathCompanyMap.get("investment_ratio_total"));
            if (investmentRatioTotal.compareTo(THRESHOLD_PERCENT_FIFTY) < 0) continue;
            // 公司实体
            String shareholderType = String.valueOf(ratioPathCompanyMap.get("shareholder_entity_type"));
            if ("2".equals(shareholderType)) continue;
            // 不是终点
            String isEnd = String.valueOf(ratioPathCompanyMap.get("is_end"));
            if ("1".equals(isEnd)) continue;
            // 不是实控人 但是 在实控人路径中出现
            String shareholderId = String.valueOf(ratioPathCompanyMap.get("shareholder_id"));
            if (!controllerMapsString.contains(shareholderId) || controllerSet.contains(shareholderId))
                continue;
            // 补充实际控制权
            controllerMaps.add(getNormalColumnMap(ratioPathCompanyMap, false));
        }
        // 控制传递
        controllerMapsString = controllerMaps.toString();
        controllerSet = controllerMaps.stream().map(e -> String.valueOf(e.get("tyc_unique_entity_id"))).collect(Collectors.toSet());
        for (Map<String, Object> ratioPathCompanyMap : ratioPathCompanyMaps) {
            String maxDeliver = String.valueOf(ratioPathCompanyMap.get("max_deliver"));
            String shareholderId = String.valueOf(ratioPathCompanyMap.get("shareholder_id"));
            if (!controllerMapsString.contains(shareholderId) || controllerSet.contains(shareholderId) || maxDeliver.compareTo(THRESHOLD_PERCENT_FIFTY) < 0)
                continue;
            // 补充实际控制权
            controllerMaps.add(getNormalColumnMap(ratioPathCompanyMap, false));
        }
    }

    private boolean isInvalidShareholder(Map<String, Object> shareholderMap) {
        return
                // 股东id
                !TycUtils.isTycUniqueEntityId(String.valueOf(shareholderMap.get("id"))) ||
                        // 股东名称
                        !TycUtils.isValidName(String.valueOf(shareholderMap.get("name"))) ||
                        // 股东内链name_id
                        !TycUtils.isUnsignedId(String.valueOf(shareholderMap.get("name_id"))) ||
                        // 股东内链company_id
                        !TycUtils.isUnsignedId(String.valueOf(shareholderMap.get("company_id")));
    }

    private Map<String, Object> getNormalColumnMap(Map<String, Object> ratioPathCompanyMap, boolean isController) {
        List<List<Map<String, Object>>> paths = getNormalJsonList(String.valueOf(ratioPathCompanyMap.get("equity_holding_path")));
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("tyc_unique_entity_id", String.valueOf(ratioPathCompanyMap.get("shareholder_id")));
        columnMap.put("entity_type_id", String.valueOf(ratioPathCompanyMap.get("shareholder_entity_type")));
        columnMap.put("entity_name_valid", String.valueOf(ratioPathCompanyMap.get("shareholder_name")));
        columnMap.put("company_id_controlled", String.valueOf(ratioPathCompanyMap.get("company_id")));
        columnMap.put("company_name_controlled", String.valueOf(ratioPathCompanyMap.get("company_name")));
        columnMap.put("equity_relation_path_cnt", paths.size());
        columnMap.put("estimated_equity_ratio_total", String.valueOf(ratioPathCompanyMap.get("investment_ratio_total")));
        columnMap.put("controlling_equity_relation_path_detail", JsonUtils.toString(paths));
        columnMap.put("is_controller_tyc_unique_entity_id", isController);
        return columnMap;
    }

    private List<List<Map<String, Object>>> getNormalJsonList(String ratioPathCompanyJson) {
        List<List<Map<String, Object>>> resultPaths = new ArrayList<>();
        for (Object pathObj : JsonUtils.parseJsonArr(ratioPathCompanyJson)) {
            List<Map<String, Object>> resultPath = new ArrayList<>();
            for (Map<String, Object> node : (List<Map<String, Object>>) pathObj) {
                Map<String, Object> resultNode = new LinkedHashMap<>();
                // 人
                if (node.containsKey("pid")) {
                    resultNode.put("id", node.get("pid"));
                    resultNode.put("properties", new LinkedHashMap<String, Object>() {{
                        put("name", node.get("name"));
                        put("node_type", "2");
                        put("human_name_id", node.get("hid"));
                        put("human_master_company_id", node.get("cid"));
                    }});
                    // 上一个node是边, 为边补充endId
                    if (!resultPath.isEmpty()) {
                        resultPath.get(resultPath.size() - 1).put("endNode", resultNode.get("id"));
                    }
                }
                // 公司
                else if (node.containsKey("cid")) {
                    resultNode.put("id", node.get("cid"));
                    resultNode.put("properties", new LinkedHashMap<String, Object>() {{
                        put("name", node.get("name"));
                        put("node_type", "1");
                        put("company_id", node.get("cid"));
                    }});
                    // 上一个node是边, 为边补充endId
                    if (!resultPath.isEmpty()) {
                        resultPath.get(resultPath.size() - 1).put("endNode", resultNode.get("id"));
                    }
                }
                // 边
                else if (node.containsKey("edges")) {
                    // 上一个node是点, 获取上一个node的id
                    resultNode.put("startNode", resultPath.get(resultPath.size() - 1).get("id"));
                    resultNode.put("endNode", "end_node");
                    resultNode.put("properties", new LinkedHashMap<String, Object>() {{
                        put("equity_ratio", new BigDecimal(((List<Map<String, Object>>) node.get("edges")).get(0).get("percent").toString().replaceAll("%", "")).divide(new BigDecimal("100"), 6, RoundingMode.DOWN).toPlainString());
                    }});
                }
                if (!resultNode.isEmpty()) {
                    resultPath.add(resultNode);
                }
            }
            resultPaths.add(resultPath);
        }
        return resultPaths;
    }

    private Map<String, Object> getSpecialColumnMap(String companyId, String companyName, Map<String, Object> shareholderMap, String ratio) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("tyc_unique_entity_id", String.valueOf(shareholderMap.get("id")));
        columnMap.put("entity_type_id", String.valueOf(shareholderMap.get("id")).length() == 17 ? "2" : "1");
        columnMap.put("entity_name_valid", String.valueOf(shareholderMap.get("name")));
        columnMap.put("company_id_controlled", companyId);
        columnMap.put("company_name_controlled", companyName);
        columnMap.put("equity_relation_path_cnt", 1);
        columnMap.put("estimated_equity_ratio_total", ratio);
        columnMap.put("controlling_equity_relation_path_detail", JsonUtils.toString(getSpecialJsonList(companyId, companyName, shareholderMap, ratio)));
        columnMap.put("is_controller_tyc_unique_entity_id", "1");
        return columnMap;
    }

    private List<List<Map<String, Object>>> getSpecialJsonList(String companyId, String companyName, Map<String, Object> shareholderMap, String ratio) {
        String shareholderId = String.valueOf(shareholderMap.get("id"));
        String shareholderName = String.valueOf(shareholderMap.get("name"));
        String shareholderNameId = String.valueOf(shareholderMap.get("name_id"));
        String humanMasterCompanyId = String.valueOf(shareholderMap.get("company_id"));
        return new ArrayList<List<Map<String, Object>>>() {{
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
    }
}
