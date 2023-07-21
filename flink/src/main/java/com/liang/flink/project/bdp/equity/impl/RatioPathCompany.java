package com.liang.flink.project.bdp.equity.impl;

import com.liang.common.service.SQL;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.bdp.equity.dao.BdpEquityDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import com.liang.flink.utils.BuildTab3Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.DELETE;

public class RatioPathCompany extends AbstractDataUpdate<SQL> {
    private final BdpEquityDao dao = new BdpEquityDao();

    @Override
    public List<SQL> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String id = String.valueOf(columnMap.get("id"));
        String companyId = String.valueOf(columnMap.get("company_id"));
        String shareholderId = String.valueOf(columnMap.get("shareholder_id"));
        //删除三张表的相关内容
        dao.deleteAll(id, companyId, shareholderId);
        String isDeleted = String.valueOf(columnMap.get("is_deleted"));
        if (singleCanalBinlog.getEventType() == DELETE || "1".equals(isDeleted)) {
            return new ArrayList<>();
        }
        String isBigShareholder = String.valueOf(columnMap.get("is_big_shareholder"));
        String isControllingShareholder = String.valueOf(columnMap.get("is_controlling_shareholder"));
        String isController = String.valueOf(columnMap.get("is_controller"));
        String isUltimate = String.valueOf(columnMap.get("is_ultimate"));
        //如果id是无效的,return
        if ("0".equals(companyId) || "".equals(companyId) || "0".equals(shareholderId) || "".equals(shareholderId)) {
            return new ArrayList<>();
        }
        //如果啥也不是,return
        if ("0".equals(isBigShareholder) && "0".equals(isControllingShareholder) && "0".equals(isController) && "0".equals(isUltimate)) {
            return new ArrayList<>();
        }
        //名字
        columnMap.put("company_name", dao.getEntityName(companyId));
        columnMap.put("shareholder_name", dao.getEntityName(shareholderId));
        //path_node
        String equityHoldingPath = String.valueOf(columnMap.get("equity_holding_path"));
        BuildTab3Path.PathNode pathNode = BuildTab3Path.buildTab3PathSafe(shareholderId, equityHoldingPath);
        columnMap.put("path_node", pathNode);
        //写入受益所有人
        if (!"0".equals(isUltimate)) {
            parseIntoEntityBeneficiaryDetails(singleCanalBinlog);
        }
        //写入实际控制人
        if (!"0".equals(isController)) {
            parseIntoEntityControllerDetails(singleCanalBinlog);
        }
        //写入公司股东关系表
        parseIntoShareholderIdentityTypeDetails(singleCanalBinlog);
        return new ArrayList<>();
    }

    // entity_beneficiary_details
    // unique (tyc_unique_entity_id_beneficiary, tyc_unique_entity_id)
    private void parseIntoEntityBeneficiaryDetails(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        BuildTab3Path.PathNode pathNode = (BuildTab3Path.PathNode) columnMap.get("path_node");
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("id", String.valueOf(columnMap.get("id")));
        resultMap.put("tyc_unique_entity_id", String.valueOf(columnMap.get("company_id")));
        resultMap.put("tyc_unique_entity_id_beneficiary", String.valueOf(columnMap.get("shareholder_id")));
        //公司名字
        resultMap.put("entity_name_valid", String.valueOf(columnMap.get("company_name")));
        //股东名字
        resultMap.put("entity_name_beneficiary", String.valueOf(columnMap.get("shareholder_name")));
        //其它信息
        resultMap.put("equity_relation_path_cnt", pathNode.getCount());
        resultMap.put("beneficiary_equity_relation_path_detail", pathNode.getPathStr());
        resultMap.put("estimated_equity_ratio_total", String.valueOf(columnMap.get("investment_ratio_total")));
        resultMap.put("beneficiary_validation_time_year", 2023);
        resultMap.put("entity_type_id", 1);
        dao.replaceInto("entity_beneficiary_details", resultMap);
    }

    // entity_controller_details
    // unique (tyc_unique_entity_id, company_id_controlled)
    private void parseIntoEntityControllerDetails(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        BuildTab3Path.PathNode pathNode = (BuildTab3Path.PathNode) columnMap.get("path_node");
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("id", String.valueOf(columnMap.get("id")));
        resultMap.put("company_id_controlled", String.valueOf(columnMap.get("company_id")));
        resultMap.put("tyc_unique_entity_id", String.valueOf(columnMap.get("shareholder_id")));
        //公司名字
        resultMap.put("company_name_controlled", String.valueOf(columnMap.get("company_name")));
        //股东名字
        resultMap.put("entity_name_valid", String.valueOf(columnMap.get("shareholder_name")));
        //其他信息
        resultMap.put("equity_relation_path_cnt", pathNode.getCount());
        resultMap.put("controlling_equity_relation_path_detail", pathNode.getPathStr());
        resultMap.put("estimated_equity_ratio_total", String.valueOf(columnMap.get("investment_ratio_total")));
        resultMap.put("control_validation_time_year", 2023);
        resultMap.put("entity_type_id", String.valueOf(columnMap.get("shareholder_entity_type")));
        // 区分实际控制人 or 实际控制权
        if ("2".equals(String.valueOf(columnMap.get("shareholder_entity_type")))) {
            resultMap.put("is_controller_tyc_unique_entity_id", 0);
        }
        dao.replaceInto("entity_controller_details", resultMap);
    }

    // shareholder_identity_type_details
    // unique (公司id 股东id 股东身份类型(4种))
    private void parseIntoShareholderIdentityTypeDetails(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        // 1-大股东 2-控股股东 3-实控人 4-受益人
        ArrayList<Integer> identities = new ArrayList<>();
        if ("1".equals(String.valueOf(columnMap.get("is_big_shareholder"))))
            identities.add(1);
        if ("1".equals(String.valueOf(columnMap.get("is_controlling_shareholder"))))
            identities.add(2);
        if ("1".equals(String.valueOf(columnMap.get("is_controller"))))
            identities.add(3);
        if ("1".equals(String.valueOf(columnMap.get("is_ultimate"))))
            identities.add(4);
        //如果啥也不是,return
        if (identities.isEmpty()) {
            return;
        }
        HashMap<String, Object> resultMap = new HashMap<>();
        // 公司信息
        resultMap.put("tyc_unique_entity_id", String.valueOf(columnMap.get("company_id")));
        resultMap.put("entity_type_id", 1);
        resultMap.put("entity_name_valid", String.valueOf(columnMap.get("company_name")));
        // 股东信息
        resultMap.put("entity_type_id_with_shareholder_identity_type", String.valueOf(columnMap.get("shareholder_entity_type")));
        resultMap.put("tyc_unique_entity_id_with_shareholder_identity_type", String.valueOf(columnMap.get("shareholder_id")));
        resultMap.put("entity_name_valid_with_shareholder_identity_type", String.valueOf(columnMap.get("shareholder_name")));
        // 身份信息
        resultMap.put("shareholder_identity_type", 0);
        // 其它信息
        BuildTab3Path.PathNode pathNode = (BuildTab3Path.PathNode) columnMap.get("path_node");
        resultMap.put("equity_ratio_path_cnt", pathNode.getCount());
        resultMap.put("estimated_equity_ratio_total", String.valueOf(columnMap.get("investment_ratio_total")));
        resultMap.put("shareholder_validation_time_year", 2023);
        List<Map<String, Object>> resultMaps = new ArrayList<>();
        for (Integer identity : identities) {
            resultMap.put("shareholder_identity_type", identity);
            resultMaps.add(new HashMap<>(resultMap));
        }
        dao.replaceIntoShareholderTypeDetail(resultMaps);
    }

    @Override
    public List<SQL> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
