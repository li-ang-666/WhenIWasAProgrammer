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
        //删除
        dao.deleteAll(id, companyId, shareholderId);
        String isDeleted = String.valueOf(columnMap.get("is_deleted"));
        if (singleCanalBinlog.getEventType() == DELETE || "1".equals(isDeleted)) {
            return new ArrayList<>();
        }
        //写入受益所有人
        String isUltimate = String.valueOf(columnMap.get("is_ultimate"));
        if (!"0".equals(isUltimate)) {
            parseIntoEntityBeneficiaryDetails(singleCanalBinlog);
        }
        //写入实际控制人
        String isController = String.valueOf(columnMap.get("is_controller"));
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
        String companyId = String.valueOf(columnMap.get("company_id"));
        String shareholderId = String.valueOf(columnMap.get("shareholder_id"));
        String id = String.valueOf(columnMap.get("id"));
        String investmentRatioTotal = String.valueOf(columnMap.get("investment_ratio_total"));
        String equityHoldingPath = String.valueOf(columnMap.get("equity_holding_path"));
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("id", id);
        resultMap.put("tyc_unique_entity_id", companyId);
        resultMap.put("tyc_unique_entity_id_beneficiary", shareholderId);
        //公司名字
        String companyName = dao.getEntityName(companyId);
        resultMap.put("entity_name_valid", companyName);
        //股东名字
        String shareholderName = dao.getEntityName(shareholderId);
        resultMap.put("entity_name_beneficiary", shareholderName);
        //其它信息
        BuildTab3Path.PathNode pathNode = BuildTab3Path.buildTab3PathSafe(shareholderId, equityHoldingPath);
        resultMap.put("equity_relation_path_cnt", pathNode.getCount());
        resultMap.put("beneficiary_equity_relation_path_detail", pathNode.getPathStr());
        resultMap.put("estimated_equity_ratio_total", investmentRatioTotal);
        resultMap.put("beneficiary_validation_time_year", 2023);
        resultMap.put("entity_type_id", 1);
        dao.replaceInto("entity_beneficiary_details", resultMap);
    }

    // entity_controller_details
    // unique (tyc_unique_entity_id, company_id_controlled)
    private void parseIntoEntityControllerDetails(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String companyId = String.valueOf(columnMap.get("company_id"));
        String shareholderId = String.valueOf(columnMap.get("shareholder_id"));
        String id = String.valueOf(columnMap.get("id"));
        String investmentRatioTotal = String.valueOf(columnMap.get("investment_ratio_total"));
        String equityHoldingPath = String.valueOf(columnMap.get("equity_holding_path"));
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("id", id);
        resultMap.put("company_id_controlled", companyId);
        resultMap.put("tyc_unique_entity_id", shareholderId);
        //公司名字
        String companyName = dao.getEntityName(companyId);
        resultMap.put("company_name_controlled", companyName);
        //股东名字
        String shareholderName = dao.getEntityName(shareholderId);
        resultMap.put("entity_name_valid", shareholderName);
        //其他信息
        BuildTab3Path.PathNode pathNode = BuildTab3Path.buildTab3PathSafe(shareholderId, equityHoldingPath);
        resultMap.put("equity_relation_path_cnt", pathNode.getCount());
        resultMap.put("controlling_equity_relation_path_detail", pathNode.getPathStr());
        resultMap.put("estimated_equity_ratio_total", investmentRatioTotal);
        resultMap.put("control_validation_time_year", 2023);
        resultMap.put("entity_type_id", String.valueOf(columnMap.get("shareholder_entity_type")));
        dao.replaceInto("entity_controller_details", resultMap);
    }

    // shareholder_identity_type_details
    // unique (公司id 股东id 股东身份类型(4种))
    private void parseIntoShareholderIdentityTypeDetails(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        // 1-大股东
        String isBigShareholder = String.valueOf(columnMap.get("is_big_shareholder"));
        // 2-控股股东
        String isControllingShareholder = String.valueOf(columnMap.get("is_controlling_shareholder"));
        // 3-实控人
        String isController = String.valueOf(columnMap.get("is_controller"));
        // 4-受益人
        String isUltimate = String.valueOf(columnMap.get("is_ultimate"));
        ArrayList<Integer> identities = new ArrayList<>();
        if ("1".equals(isBigShareholder)) {
            identities.add(1);
        }
        if ("1".equals(isControllingShareholder)) {
            identities.add(2);
        }
        if ("1".equals(isController)) {
            identities.add(3);
        }
        if ("1".equals(isUltimate)) {
            identities.add(4);
        }
        for (Integer identity : identities) {

        }
    }

    @Override
    public List<SQL> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
