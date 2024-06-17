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
    private static final String REASON_LISTED = "上市公告披露";
    private static final String REASON_EQUITY_DIRECT = "直接持股";
    private static final String REASON_EQUITY_INDIRECT = "间接持股";
    private static final String REASON_EQUITY_ALL = "直接/间接持股";
    private static final String REASON_PERSONNEL = "关键决策人员";
    private static final String REASON_PARTNER = "执行事务合伙人";
    private static final String THRESHOLD_PERCENT_THIRTY = "0.300000";
    private static final String THRESHOLD_PERCENT_FIFTY = "0.500000";
    private static final String[] USCC_WHITE_LIST = new String[]{"31", "91", "92", "93"};
    private final EquityControlDao controllerDao = new EquityControlDao();
    private final EquityBfsDao bfsDao = new EquityBfsDao();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        for (Map<String, Object> columnMap : new EquityControlService().processControl("2426389306")) {
            System.out.println(StrUtil.repeat("=", 100));
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
        String usccPrefixTwo = String.valueOf(companyInfo.get("unified_social_credit_code"));
        if (!StrUtil.startWithAny(usccPrefixTwo, USCC_WHITE_LIST)) return columnMaps;
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
                    ratio = new BigDecimal(holdingRatio).divide(new BigDecimal("100"), 6, RoundingMode.DOWN).toPlainString();
                } catch (Exception ignore) {
                    ratio = "0.000000";
                }
                // 查询股东维表
                Map<String, Object> shareholderMap = bfsDao.queryHumanOrCompanyInfo(listedAnnouncedControllerId);
                // 验证股东维表
                if (isInvalidShareholder(shareholderMap)) continue;
                // 构造columnMap
                columnMaps.add(getSpecialColumnMap(companyId, companyName, shareholderMap, ratio, REASON_LISTED, REASON_LISTED));
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
                // 筛选比例>=30%的股东
                String ratio = String.valueOf(ratioPathCompanyMap.get("investment_ratio_total"));
                if (ratio.compareTo(THRESHOLD_PERCENT_THIRTY) < 0) continue;
                ratioToRatioPathCompanyMapsWithSameRatio.compute(ratio, (k, v) -> {
                    List<Map<String, Object>> RatioPathCompanyMapsWithSameRatio = (v != null) ? v : new ArrayList<>();
                    RatioPathCompanyMapsWithSameRatio.add(ratioPathCompanyMap);
                    return RatioPathCompanyMapsWithSameRatio;
                });
            }
            // 最后一个entry的value(最大比例下的股东list)
            List<Map<String, Object>> ratioPathCompanyMapsWithMaxRatio = ratioToRatioPathCompanyMapsWithSameRatio.isEmpty() ?
                    new ArrayList<>() :
                    ratioToRatioPathCompanyMapsWithSameRatio.lastEntry().getValue();
            // 最大比例股东有且只有一位
            if (ratioPathCompanyMapsWithMaxRatio.size() == 1) {
                // 无论该最终股东是否为自然人, 均可为实际控制人
                columnMaps.add(getNormalColumnMap(ratioPathCompanyMapsWithMaxRatio.get(0), true, "唯一最大股东"));
            }
            // 最大比例股东有多位
            else if (ratioPathCompanyMapsWithMaxRatio.size() > 1) {
                for (Map<String, Object> ratioPathCompanyMap : ratioPathCompanyMapsWithMaxRatio) {
                    String shareholderType = String.valueOf(ratioPathCompanyMap.get("shareholder_entity_type"));
                    String shareholderId = String.valueOf(ratioPathCompanyMap.get("shareholder_id"));
                    // 非自然人, 则直接为实际控制人
                    if (shareholderType.equals("1")) {
                        columnMaps.add(getNormalColumnMap(ratioPathCompanyMap, true, "并列最大股东/非自然人"));
                    }
                    // 自然人, 判断该自然人是否在当前企业担任 董事长、执行董事 职位
                    else if (shareholderType.equals("2")) {
                        String position = controllerDao.queryChairMan(companyId, shareholderId);
                        if (position == null) continue;
                        columnMaps.add(getNormalColumnMap(ratioPathCompanyMap, true, "并列最大股东/自然人/" + position));
                    }
                }
            }
            // 经过以上判断, 未发现实控人, 选用董事长 or 执行事务合伙人 为 实控人
            if (columnMaps.isEmpty()) {
                boolean isPartnership = controllerDao.isPartnership(companyId);
                List<Map<String, Object>> vips = isPartnership ? controllerDao.queryAllPartners(companyId) : controllerDao.queryAllPersonnels(companyId);
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
                    if (isPartnership) {
                        columnMaps.add(getSpecialColumnMap(companyId, companyName, shareholderMap, ratio, REASON_PARTNER, String.valueOf(vip.get("position"))));
                    } else {
                        columnMaps.add(getSpecialColumnMap(companyId, companyName, shareholderMap, ratio, REASON_PERSONNEL + "(" + vip.get("position") + ")", String.valueOf(vip.get("position"))));
                    }
                }
            }
            // 补充实际控制权
            fillUpControlAbility(ratioPathCompanyMaps, columnMaps);
        }
        return columnMaps;
    }

    private void fillUpControlAbility(List<Map<String, Object>> ratioPathCompanyMaps, List<Map<String, Object>> controllerMaps) {
        // 实控人路径中间节点
        String controllerString = controllerMaps.toString();
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
            if (controllerString.contains(shareholderId) & !controllerSet.contains(shareholderId)) {
                // 补充实际控制权
                controllerMaps.add(getNormalColumnMap(ratioPathCompanyMap, false, "补充实控权/实控人路径"));
            }
        }
        // 控制传递
        controllerSet = controllerMaps.stream().map(e -> String.valueOf(e.get("tyc_unique_entity_id"))).collect(Collectors.toSet());
        for (Map<String, Object> ratioPathCompanyMap : ratioPathCompanyMaps) {
            String maxDeliver = String.valueOf(ratioPathCompanyMap.get("max_deliver"));
            String shareholderId = String.valueOf(ratioPathCompanyMap.get("shareholder_id"));
            if (!controllerSet.contains(shareholderId) && maxDeliver.compareTo(THRESHOLD_PERCENT_FIFTY) >= 0) {
                // 补充实际控制权
                controllerMaps.add(getNormalColumnMap(ratioPathCompanyMap, false, "补充实控权/控制传递"));
            }
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

    private Map<String, Object> getNormalColumnMap(Map<String, Object> ratioPathCompanyMap, boolean isController, String reasonDetail) {
        List<List<Map<String, Object>>> paths = getNormalJsonList(String.valueOf(ratioPathCompanyMap.get("equity_holding_path")));
        int pathCount = paths.size();
        int min = paths.stream().map(List::size).min(Comparator.comparingInt(i -> i)).orElse(0);
        int max = paths.stream().map(List::size).max(Comparator.comparingInt(i -> i)).orElse(0);
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("tyc_unique_entity_id", String.valueOf(ratioPathCompanyMap.get("shareholder_id")));
        columnMap.put("entity_type_id", String.valueOf(ratioPathCompanyMap.get("shareholder_entity_type")));
        columnMap.put("entity_name_valid", String.valueOf(ratioPathCompanyMap.get("shareholder_name")));
        columnMap.put("company_id_controlled", String.valueOf(ratioPathCompanyMap.get("company_id")));
        columnMap.put("company_name_controlled", String.valueOf(ratioPathCompanyMap.get("company_name")));
        columnMap.put("equity_relation_path_cnt", pathCount);
        columnMap.put("estimated_equity_ratio_total", String.valueOf(ratioPathCompanyMap.get("investment_ratio_total")));
        // 明细json只要前10
        columnMap.put("controlling_equity_relation_path_detail", JsonUtils.toString(paths.size() <= 10 ? paths : paths.subList(0, 10)));
        columnMap.put("is_controller_tyc_unique_entity_id", isController);
        if (min == 3 && max == 3) {
            columnMap.put("reason", REASON_EQUITY_DIRECT);
        } else if (min > 3 && max > 3) {
            columnMap.put("reason", REASON_EQUITY_INDIRECT);
        } else {
            columnMap.put("reason", REASON_EQUITY_ALL);
        }
        columnMap.put("reason_detail", reasonDetail);
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

    private Map<String, Object> getSpecialColumnMap(String companyId, String companyName, Map<String, Object> shareholderMap, String ratio, String reason, String reasonDetail) {
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
        columnMap.put("reason", reason);
        columnMap.put("reason_detail", reasonDetail);
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
