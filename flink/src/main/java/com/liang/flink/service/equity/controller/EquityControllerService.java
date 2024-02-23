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
    private static final BigDecimal THRESHOLD_PERCENT_THIRTY = new BigDecimal("0.3");
    private static final BigDecimal THRESHOLD_PERCENT_FIFTY = new BigDecimal("0.5");
    private static final Set<String> USCC_TWO_WHITE_LIST = new HashSet<>(Arrays.asList("31", "91", "92", "93"));
    private final EquityControllerDao controllerDao = new EquityControllerDao();
    private final EquityBfsDao bfsDao = new EquityBfsDao();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        List<Map<String, Object>> columnMaps = new EquityControllerService().processController("14427175");
        for (Map<String, Object> columnMap : columnMaps) {
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
    }

    public List<Map<String, Object>> processController(String companyId) {
        List<Map<String, Object>> columnMaps = new ArrayList<>();
        // 非法id
        if (!TycUtils.isUnsignedId(companyId)) return columnMaps;
        // 查询company_index
        Map<String, Object> companyInfo = controllerDao.queryCompanyInfo(companyId);
        // 在company_index不存在
        if (companyInfo.isEmpty()) return columnMaps;
        // 非法名称
        String companyName = String.valueOf(companyInfo.get("company_name"));
        if (!TycUtils.isValidName(companyName)) return columnMaps;
        // 企业类型屏蔽逻辑
        String usccPrefixTwo = (companyInfo.get("unified_social_credit_code") + "suffix to prevent OutOfBoundsException").substring(0, 2);
        if (!USCC_TWO_WHITE_LIST.contains(usccPrefixTwo)) return columnMaps;
        // 上市公告的实际控制人
        List<Map<String, Object>> listedAnnouncedControllers = controllerDao.queryListedAnnouncedControllers(companyId);
        if (!listedAnnouncedControllers.isEmpty()) {
            for (Map<String, Object> listedAnnouncedController : listedAnnouncedControllers) {
                String controllerType = String.valueOf(listedAnnouncedController.get("controller_type"));
                String controllerGid = String.valueOf(listedAnnouncedController.get("controller_gid"));
                String controllerPid = String.valueOf(listedAnnouncedController.get("controller_pid"));
                String holdingRatio = String.valueOf(listedAnnouncedController.get("holding_ratio"));
                // 排除非人非公司 0-人 1-公司
                if (!StrUtil.equalsAny(controllerType, "0", "1")) continue;
                // 公司必须有gid
                if ("1".equals(controllerType) && !TycUtils.isUnsignedId(controllerGid)) continue;
                // 自然人必须有pid
                if ("0".equals(controllerType) && controllerPid.length() != 17) continue;
                // 必须有ratio
                String ratio;
                try {
                    ratio = new BigDecimal(holdingRatio).setScale(6, RoundingMode.DOWN).toPlainString();
                } catch (Exception ignore) {
                    continue;
                }
                // 查询股东维表
                Map<String, Object> shareholderInfo = ("1".equals(controllerType)) ?
                        bfsDao.queryHumanOrCompanyInfo(controllerGid, "1") :
                        bfsDao.queryHumanOrCompanyInfo(controllerPid, "2");
                // 在维表不存在
                if (!isValidShareholder(shareholderInfo)) continue;
                // 构造columnMap
                columnMaps.add(getColumnMap(companyId, companyName, String.valueOf(shareholderInfo.get("id")), String.valueOf(shareholderInfo.get("name")), String.valueOf(shareholderInfo.get("name_id")), String.valueOf(shareholderInfo.get("company_id")), ratio));
            }
            return columnMaps;
        }
        // 股权穿透
        List<Map<String, Object>> shareholders = controllerDao.queryRatioPathCompany(companyId);
        // 没有股东 or 股东是自己(个体工商户)
        if (shareholders.isEmpty() || (shareholders.size() == 1 && String.valueOf(shareholders.get(0).get("company_id")).equals(String.valueOf(shareholders.get(0).get("shareholder_id"))))) {
            List<Map<String, Object>> personnels = controllerDao.queryPersonnel(companyId);
            // 没有董事
            if (personnels.isEmpty()) return columnMaps;
            // 有董事
            for (Map<String, Object> personnel : personnels) {
                String shareholderId = String.valueOf(personnel.get("human_id"));
                // 非法id
                if (!TycUtils.isTycUniqueEntityId(shareholderId)) continue;
                // 查询股东维表
                Map<String, Object> shareholderInfo = bfsDao.queryHumanOrCompanyInfo(shareholderId, "2");
                // 在维表不存在
                if (!isValidShareholder(shareholderInfo)) continue;
                // 构造columnMap
                columnMaps.add(getColumnMap(companyId, companyName, String.valueOf(shareholderInfo.get("id")), String.valueOf(shareholderInfo.get("name")), String.valueOf(shareholderInfo.get("name_id")), String.valueOf(shareholderInfo.get("company_id")), "0"));
            }
            return columnMaps;
        }
        // 有股东
        // 判断最大股比例
        BigDecimal maxInvestmentRatioTotal = shareholders.stream().map(e -> new BigDecimal(String.valueOf(e.get("investment_ratio_total")))).max(BigDecimal::compareTo).orElse(BigDecimal.ZERO);


        return columnMaps;
    }

    private boolean isValidShareholder(Map<String, Object> shareholderInfo) {
        return
                // 股东id
                TycUtils.isTycUniqueEntityId(String.valueOf(shareholderInfo.get("id"))) &&
                        // 股东名称
                        TycUtils.isValidName(String.valueOf(shareholderInfo.get("name"))) &&
                        // 股东内链1
                        TycUtils.isUnsignedId(String.valueOf(shareholderInfo.get("name_id"))) &&
                        // 股东内链2
                        TycUtils.isUnsignedId(String.valueOf(shareholderInfo.get("company_id")));
    }

    private Map<String, Object> getColumnMap(String companyId, String companyName, String shareholderId, String shareholderName, String shareholderNameId, String humanMasterCompanyId, String ratio) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("tyc_unique_entity_id", shareholderId);
        columnMap.put("entity_type_id", shareholderId.length() == 17 ? "2" : "1");
        columnMap.put("entity_name_valid", shareholderName);
        columnMap.put("company_id_controlled", companyId);
        columnMap.put("company_name_controlled", companyName);
        columnMap.put("equity_relation_path_cnt", 1);
        columnMap.put("estimated_equity_ratio_total", ratio);
        columnMap.put("controlling_equity_relation_path_detail", getJson(companyId, companyName, shareholderId, shareholderName, shareholderNameId, humanMasterCompanyId, ratio));
        columnMap.put("control_validation_time_year", "2023");
        columnMap.put("is_controller_tyc_unique_entity_id", "1");
        return columnMap;
    }

    private String getJson(String companyId, String companyName, String shareholderId, String shareholderName, String shareholderNameId, String humanMasterCompanyId, String ratio) {
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
