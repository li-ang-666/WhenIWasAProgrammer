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
                } catch (Exception e) {
                    continue;
                }
                // 查询名字
                Map<String, Object> shareholderInfo = ("1".equals(controllerType)) ?
                        bfsDao.queryHumanOrCompanyInfo(controllerGid, "1") :
                        bfsDao.queryHumanOrCompanyInfo(controllerPid, "2");
                String shareholderName = String.valueOf(shareholderInfo.get("name"));
                // 非法名称
                if (!TycUtils.isValidName(shareholderName)) continue;
                // 非法name_id
                String nameId = String.valueOf(shareholderInfo.get("name_id"));
                if (!TycUtils.isUnsignedId(nameId)) continue;
                // 非法master_company_id
                String masterCompanyId = String.valueOf(shareholderInfo.get("company_id"));
                if (!TycUtils.isUnsignedId(masterCompanyId)) continue;
                String shareholderId = String.valueOf(shareholderInfo.get("id"));
                // 构造columnMap
                columnMaps.add(getColumnMap(companyId, companyName, shareholderId, shareholderName, nameId, masterCompanyId, ratio));
            }
            return columnMaps;
        }
        // 股权穿透
        List<Map<String, Object>> shareholders = controllerDao.queryRatioPathCompany(companyId);

        return columnMaps;
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
