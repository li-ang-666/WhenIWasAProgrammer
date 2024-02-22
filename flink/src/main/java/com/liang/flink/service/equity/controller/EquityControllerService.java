package com.liang.flink.service.equity.controller;

import cn.hutool.core.util.StrUtil;
import com.liang.common.util.TycUtils;
import com.liang.flink.service.equity.bfs.EquityBfsDao;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class EquityControllerService {
    private static final Set<String> USCC_TWO_WHITE_LIST = new HashSet<>(Arrays.asList("31", "91", "92", "93"));
    private final EquityControllerDao controllerDap = new EquityControllerDao();
    private final EquityBfsDao bfsDao = new EquityBfsDao();

    public void processController(String companyId) {
        List<Map<String, Object>> columnMaps = new ArrayList<>();
        // 非法id
        if (!TycUtils.isUnsignedId(companyId)) {
            return;
        }
        // 查询company_index
        Map<String, Object> companyInfo = controllerDap.queryCompanyInfo(companyId);
        // 在company_index不存在
        if (companyInfo.isEmpty()) {
            return;
        }
        // 非法名称
        String companyName = String.valueOf(companyInfo.get("company_name"));
        if (!TycUtils.isValidName(companyName)) {
            return;
        }
        // 企业类型屏蔽逻辑
        String usccPrefixTwo = String.valueOf(companyInfo.get("unified_social_credit_code")).substring(0, 2);
        if (!USCC_TWO_WHITE_LIST.contains(usccPrefixTwo)) {
            return;
        }
        // 上市公告的实际控制人
        List<Map<String, Object>> listedAnnouncedControllers = controllerDap.queryListedAnnouncedControllers(companyId);
        if (!listedAnnouncedControllers.isEmpty()) {
            for (Map<String, Object> listedAnnouncedController : listedAnnouncedControllers) {
                String controllerType = String.valueOf(listedAnnouncedController.get("controller_type"));
                String controllerGid = String.valueOf(listedAnnouncedController.get("controller_gid"));
                String controllerPid = String.valueOf(listedAnnouncedController.get("controller_pid"));
                String holdingRatio = String.valueOf(listedAnnouncedController.get("holding_ratio"));
                // 排除非人非公司
                if (!StrUtil.equalsAny(controllerType, "0", "1")) continue;
                // 公司必须有gid
                if ("0".equals(controllerType) && !TycUtils.isUnsignedId(controllerGid)) continue;
                // 自然人必须有pid
                if ("1".equals(controllerType) && controllerPid.length() != 17) continue;
                // 必须有ratio
                BigDecimal ratio;
                try {
                    ratio = new BigDecimal(holdingRatio);
                } catch (Exception e) {
                    continue;
                }
                // 查询名字
                Map<String, Object> shareholderInfo = ("0".equals(controllerType)) ?
                        bfsDao.queryHumanOrCompanyInfo(controllerGid, "1") :
                        bfsDao.queryHumanOrCompanyInfo(controllerPid, "2");
                String controllerName = String.valueOf(shareholderInfo.get("name"));
                // 非法名称
                if (!TycUtils.isValidName(controllerName)) continue;
                // 非法name_id
                String nameId = String.valueOf(shareholderInfo.get("name_id"));
                if (!TycUtils.isUnsignedId(nameId)) continue;
                // 非法master_company_id
                String masterCompanyId = String.valueOf(shareholderInfo.get("company_id"));
                if (!TycUtils.isUnsignedId(masterCompanyId)) continue;
                // 构造columnMap
                HashMap<String, Object> columnMap = new HashMap<>();
                columnMap.put("tyc_unique_entity_id", String.valueOf(shareholderInfo.get("id")));
                columnMap.put("entity_type_id", ("0".equals(controllerType)) ? "1" : "2");
                columnMap.put("entity_name_valid", controllerName);
                columnMap.put("company_id_controlled", companyId);
                columnMap.put("company_name_controlled", companyName);
                columnMap.put("equity_relation_path_cnt", 1);
                columnMap.put("estimated_equity_ratio_total", ratio.setScale(6, RoundingMode.DOWN).toPlainString());
                columnMap.put("controlling_equity_relation_path_detail", "???");
                columnMap.put("control_validation_time_year", "2023");
                columnMap.put("is_controller_tyc_unique_entity_id", "1");
            }
        }
    }
}
