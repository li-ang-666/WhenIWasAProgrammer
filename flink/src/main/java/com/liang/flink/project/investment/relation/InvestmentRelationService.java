package com.liang.flink.project.investment.relation;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class InvestmentRelationService {
    private final InvestmentRelationDao dao = new InvestmentRelationDao();

    public List<SQL> invoke(String companyGid) {
        // 非合法ID直接跳出, 合法ID生成deleteSQL
        ArrayList<SQL> sqls = new ArrayList<>();
        if (!TycUtils.isUnsignedId(companyGid)) {
            return sqls;
        }
        SQL deleteSQL = new SQL()
                .DELETE_FROM("investment_relation")
                .WHERE("company_id_invested = " + SqlUtils.formatValue(companyGid));
        sqls.add(deleteSQL);
        // 补充主体公司信息, 主体异常, 直接跳出
        Map<String, Object> companyMap = this.getCompanyMap(companyGid);
        if (companyMap.isEmpty()) {
            return sqls;
        }
        Map<String, Object> abstractColumnMap = new HashMap<>();
        abstractColumnMap.put("company_id_invested", companyMap.get("id"));
        abstractColumnMap.put("company_name_invested", companyMap.get("name"));
        abstractColumnMap.put("company_type_invested", companyMap.get("type"));
        abstractColumnMap.put("is_listed_company_invested", companyMap.get("isListed"));
        abstractColumnMap.put("is_foreign_branches_invested", companyMap.get("isForeign"));
        abstractColumnMap.put("listed_company_actual_controller_invested", companyMap.get("listedController"));
        abstractColumnMap.put("is_partnership_company_invested", companyMap.get("isPartnership"));
        abstractColumnMap.put("legal_rep_inlinks_invested", companyMap.get("legal"));
        abstractColumnMap.put("company_uscc_prefix_code_two_invested", companyMap.get("prefix"));
        List<InvestmentRelationDao.InvestmentRelationBean> relations = dao.getRelations(companyGid);
        // 如果判断无有效股东, 补加一个relation
        if (relations.isEmpty()) {
            relations.add(new InvestmentRelationDao.InvestmentRelationBean(companyGid, companyGid, "0.01"));
        }
        // 生成每一条 investment_relation
        for (InvestmentRelationDao.InvestmentRelationBean relation : relations) {
            String shareholderGid = relation.getShareholderGid();
            String shareholderPid = relation.getShareholderPid();
            String equityRatio = relation.getEquityRatio();
            HashMap<String, Object> columnMap = new HashMap<>(abstractColumnMap);
            if (shareholderPid.length() >= 17) {
                // 股东类型是人, 通过查询名字来做验证
                String humanName = dao.getHumanName(shareholderPid);
                if (!TycUtils.isValidName(humanName)) {
                    continue;
                }
                columnMap.put("shareholder_entity_type", 2);
                columnMap.put("company_entity_inlink", humanName + ":" + shareholderGid + "-" + companyGid + ":" + shareholderPid + ":human");
                columnMap.put("shareholder_company_position_list_clean", dao.getCompanyHumanPosition(companyGid, shareholderGid));
            } else {
                // 股东类型是公司, 通过补充公司信息来做验证
                Map<String, Object> shareholderMapForCompanyType;
                // 判断是否是无股东补加的relation, 减少重复计算
                if (shareholderGid.equals(companyGid) && equityRatio.equals("0.01")) {
                    shareholderMapForCompanyType = companyMap;
                } else {
                    shareholderMapForCompanyType = getCompanyMap(shareholderGid);
                }
                if (shareholderMapForCompanyType.isEmpty()) {
                    continue;
                }
                columnMap.put("shareholder_entity_type", 1);
                columnMap.put("company_entity_inlink", shareholderMapForCompanyType.get("name") + ":" + shareholderGid + ":company");
                columnMap.put("company_type", shareholderMapForCompanyType.get("type"));
                columnMap.put("is_listed_company", shareholderMapForCompanyType.get("isListed"));
                columnMap.put("is_foreign_branches", shareholderMapForCompanyType.get("isForeign"));
                columnMap.put("listed_company_actual_controller", shareholderMapForCompanyType.get("listedController"));
                columnMap.put("is_partnership_company", shareholderMapForCompanyType.get("isPartnership"));
                columnMap.put("legal_rep_inlinks", shareholderMapForCompanyType.get("legal"));
                columnMap.put("company_uscc_prefix_code_two", shareholderMapForCompanyType.get("prefix"));
                columnMap.put("shareholder_company_position_list_clean", "");
            }
            columnMap.put("shareholder_name_id", shareholderGid);
            columnMap.put("investment_ratio", equityRatio);
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            SQL insertSQL = new SQL()
                    .INSERT_INTO("investment_relation")
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1);
            sqls.add(insertSQL);
        }
        return sqls;
    }

    private Map<String, Object> getCompanyMap(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return new HashMap<>();
        }
        // 查询 company_index, 查不到则忽略这条数据
        InvestmentRelationDao.CompanyIndexBean companyIndexBean = dao.getCompanyIndex(companyGid);
        String type = companyIndexBean.getType();
        String prefix = companyIndexBean.getPrefix();
        String isForeign = companyIndexBean.getIsForeign();
        String isPartnership = companyIndexBean.getIsPartnership();
        String name = companyIndexBean.getName();
        if (!TycUtils.isValidName(name)) {
            return new HashMap<>();
        }
        // 查询 company_legal_person (公司法人)
        String legal = dao.getLegal(companyGid);
        // 查询 company_bond_plates (是否上市)
        String isListed = dao.getIsListed(companyGid);
        // 查询 stock_actual_controller (上市公司实际控制人)
        String listedController = "";
        if ("1".equals(isListed)) {
            listedController = dao.listedController(companyGid);
        }
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put("id", companyGid);
        infoMap.put("name", name);
        infoMap.put("link", name + ":" + companyGid + ":company");
        infoMap.put("type", type);
        infoMap.put("prefix", prefix);
        infoMap.put("isForeign", isForeign);
        infoMap.put("isPartnership", isPartnership);
        infoMap.put("legal", legal);
        infoMap.put("isListed", isListed);
        infoMap.put("listedController", listedController);
        return infoMap;
    }
}
