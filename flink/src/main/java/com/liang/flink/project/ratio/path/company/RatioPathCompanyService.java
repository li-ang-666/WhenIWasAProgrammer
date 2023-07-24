package com.liang.flink.project.ratio.path.company;

import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.tyc.QueryAllShareHolderFromCompanyIdObj;
import org.tyc.bdpequity.BuildTab3Path;
import org.tyc.entity.RatioPathCompany;
import org.tyc.mybatis.mapper.company_legal_person.CompanyLegalPersonMapper;
import org.tyc.mybatis.mapper.share_holder_label.InvestmentRelationMapper;
import org.tyc.mybatis.mapper.share_holder_label.PersonnelEmploymentHistoryMapper;
import org.tyc.mybatis.runner.JDBCRunner;
import org.tyc.utils.InvestUtil;
import org.tyc.utils.PathFormatter;

import java.util.*;

@Slf4j
public class RatioPathCompanyService {
    private final InvestmentRelationMapper investmentRelationMapper = JDBCRunner.getMapper(InvestmentRelationMapper.class, "e1d4c0a1d8d1456ba4b461ab8b9f293din01/prism_shareholder_path.xml");
    private final PersonnelEmploymentHistoryMapper personnelEmploymentHistoryMapper = JDBCRunner.getMapper(PersonnelEmploymentHistoryMapper.class, "36c607bfd9174d4e81512aa73375f0fain01/human_base.xml");
    private final CompanyLegalPersonMapper companyLegalPersonMapper = JDBCRunner.getMapper(CompanyLegalPersonMapper.class, "ee59dd05fc0f4bb9a2497c8d9146a53cin01/company_base.xml");
    private final RatioPathCompanyDao dao = new RatioPathCompanyDao();

    public void invoke(Set<Long> companyIds) {
        if (companyIds.isEmpty()) {
            return;
        }
        List<String> sqls = new ArrayList<>();
        try {
            Set<Long> needDeleted = new HashSet<>(companyIds);
            companyIds.stream()
                    .filter(e -> e != 0)
                    .map(companyId -> {
                        try {
                            return QueryAllShareHolderFromCompanyIdObj.queryAllShareHolderFromCompanyId(companyId, 10000, investmentRelationMapper, personnelEmploymentHistoryMapper, companyLegalPersonMapper);
                        } catch (Exception e) {
                            log.error("queryAllShareHolderFromCompanyId({}) error", companyId);
                            return null;
                        }
                    })
                    .filter(map -> map != null && !map.isEmpty())
                    .flatMap(map -> map.values().stream())
                    .map(investmentRelation -> {
                        RatioPathCompany ratioPathCompany = InvestUtil.convertInvestmentRelationToRatioPathCompany(investmentRelation);
                        JSONArray jsonArray;
                        try {
                            jsonArray = PathFormatter.formatInvestmentRelationToPath(investmentRelation);
                        } catch (Exception e) {
                            log.error("formatInvestmentRelationToPath({}) error", investmentRelation);
                            jsonArray = new JSONArray();
                        }
                        return Tuple2.of(ratioPathCompany, jsonArray);
                    })
                    .filter(tuple2 -> {
                        RatioPathCompany ratioPathCompany = tuple2.f0;
                        Long companyId = ratioPathCompany.getCompanyId();
                        Integer shareholderEntityType = ratioPathCompany.getShareholderEntityType();
                        String shareholderId = ratioPathCompany.getShareholderId();
                        Integer isDeleted = ratioPathCompany.getIsDeleted();
                        return companyId != null && companyId != 0
                                && (shareholderEntityType == 1 || shareholderEntityType == 2)
                                && !"".equals(shareholderId) && !"0".equals(shareholderId)
                                && isDeleted == 0;
                    })
                    .forEach(tuple2 -> {
                        RatioPathCompany ratioPathCompany = tuple2.f0;
                        JSONArray path = tuple2.f1;
                        Long companyId = ratioPathCompany.getCompanyId();
                        String shareholderId = ratioPathCompany.getShareholderId();
                        if (needDeleted.contains(companyId)) {
                            dao.deleteAll(companyId);
                            needDeleted.remove(companyId);
                        }
                        Map<String, Object> columnMap = new HashMap<>();
                        columnMap.put("id", System.currentTimeMillis());
                        columnMap.put("company_id", companyId);
                        columnMap.put("shareholder_id", shareholderId);
                        columnMap.put("shareholder_entity_type", ratioPathCompany.getShareholderEntityType());
                        columnMap.put("shareholder_name_id", ratioPathCompany.getShareholderNameId());
                        columnMap.put("investment_ratio_total", ratioPathCompany.getInvestmentRatioTotal());
                        columnMap.put("is_controller", ratioPathCompany.getIsController());
                        columnMap.put("is_ultimate", ratioPathCompany.getIsUltimate());
                        columnMap.put("is_big_shareholder", ratioPathCompany.getIsBigShareholder());
                        columnMap.put("is_controlling_shareholder", ratioPathCompany.getIsControllingShareholder());
                        columnMap.put("equity_holding_path", path.toString());
                        columnMap.put("is_deleted", ratioPathCompany.getIsDeleted());
                        dao.replaceIntoRatioPathCompany(columnMap);
                        sqls.addAll(updateWithReturn(columnMap));
                    });
            needDeleted.forEach(dao::deleteAll);
            dao.updateAll(sqls);
        } catch (Exception e) {
            log.error("invoke({}) error", companyIds);
            companyIds.forEach(dao::deleteAll);
        }
    }

    public List<String> updateWithReturn(Map<String, Object> columnMap) {
        List<String> sqls = new ArrayList<>();
        String companyId = String.valueOf(columnMap.get("company_id"));
        String shareholderId = String.valueOf(columnMap.get("shareholder_id"));
        String isDeleted = String.valueOf(columnMap.get("is_deleted"));
        //如果id是无效的,return
        if ("0".equals(companyId) || "".equals(companyId) || "0".equals(shareholderId) || "".equals(shareholderId) || !"0".equals(isDeleted)) {
            return new ArrayList<>();
        }
        String isBigShareholder = String.valueOf(columnMap.get("is_big_shareholder"));
        String isControllingShareholder = String.valueOf(columnMap.get("is_controlling_shareholder"));
        String isController = String.valueOf(columnMap.get("is_controller"));
        String isUltimate = String.valueOf(columnMap.get("is_ultimate"));
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
            sqls.add(parseIntoEntityBeneficiaryDetails(columnMap));
        }
        //写入实际控制人
        if (!"0".equals(isController)) {
            sqls.add(parseIntoEntityControllerDetails(columnMap));
        }
        //写入公司股东关系表
        sqls.addAll(parseIntoShareholderIdentityTypeDetails(columnMap));
        return sqls;
    }

    // entity_beneficiary_details
    // unique (tyc_unique_entity_id_beneficiary, tyc_unique_entity_id)
    private String parseIntoEntityBeneficiaryDetails(Map<String, Object> columnMap) {
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
        return dao.replaceInto("entity_beneficiary_details", resultMap);
    }

    // entity_controller_details
    // unique (tyc_unique_entity_id, company_id_controlled)
    private String parseIntoEntityControllerDetails(Map<String, Object> columnMap) {
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
        return dao.replaceInto("entity_controller_details", resultMap);
    }

    // shareholder_identity_type_details
    // unique (公司id 股东id 股东身份类型(4种))
    private List<String> parseIntoShareholderIdentityTypeDetails(Map<String, Object> columnMap) {
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
        return dao.replaceIntoShareholderTypeDetail(resultMaps);
    }
}
