package com.liang.flink.project.investment.relation;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class InvestmentRelationDao {
    private final JdbcTemplate graphData = new JdbcTemplate("430.graph_data");
    private final JdbcTemplate companyBase = new JdbcTemplate("435.company_base");
    private final JdbcTemplate dataListedCompany = new JdbcTemplate("110.data_listed_company");
    private final JdbcTemplate humanBase = new JdbcTemplate("040.human_base");
    private final JdbcTemplate prism116 = new JdbcTemplate("116.prism");

    public List<InvestmentRelationBean> getRelations(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return new ArrayList<>();
        }
        String sql = new SQL()
                .SELECT("company_id_investor", "tyc_unique_entity_id_investor", "max(equity_ratio)")
                .FROM("company_equity_relation_details")
                .WHERE("reference_pt_year = '2023'")
                .WHERE("investor_identity_type in (1,2)")
                .WHERE("equity_ratio >= 0.05")
                .WHERE("company_id_investor > 0")
                .WHERE("tyc_unique_entity_id_investor is not null")
                .WHERE("tyc_unique_entity_id_investor <> ''")
                .WHERE("tyc_unique_entity_id_investor <> '0'")
                .WHERE("tyc_unique_entity_name_investor is not null")
                .WHERE("tyc_unique_entity_name_investor <> ''")
                .WHERE("company_id_invested = " + SqlUtils.formatValue(companyGid))
                .GROUP_BY("company_id_investor", "tyc_unique_entity_id_investor")
                .toString();
        return graphData.queryForList(sql, rs -> {
                    String shareholderGid = String.valueOf(rs.getString(1));
                    String shareholderPid = String.valueOf(rs.getString(2));
                    String ratio = new BigDecimal(String.valueOf(rs.getString(3)))
                            .setScale(12, RoundingMode.DOWN).toPlainString();
                    if (TycUtils.isUnsignedId(shareholderGid) && TycUtils.isTycUniqueEntityId(shareholderPid)) {
                        InvestmentRelationBean investmentRelationBean = new InvestmentRelationBean();
                        investmentRelationBean.setShareholderGid(shareholderGid);
                        investmentRelationBean.setShareholderPid(shareholderPid);
                        investmentRelationBean.setEquityRatio(ratio);
                        return investmentRelationBean;
                    } else {
                        return null;
                    }
                })
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public CompanyIndexBean getCompanyIndex(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return new CompanyIndexBean();
        }
        String sql = new SQL()
                .SELECT("org_type")
                .SELECT("substring(unified_social_credit_code,1,2)")
                .SELECT("case when (org_type like '%外国%分%' or org_type like '%外国%驻%' or org_type like '%外国%境内%')" +
                        " and org_type not like '%外国法人独资%' and org_type not like '%外国自然人独资%'" +
                        " and org_type not like '%台湾澳与外国投资者合资%' and org_type not like '%新闻%'" +
                        " and org_type not like '%旅游%' and org_type not like '%外国非法人%' then 1 else 0 end")
                .SELECT("if(company_type=11,1,0)")
                .SELECT("company_name")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        CompanyIndexBean res = companyBase.queryForObject(sql, rs -> {
            String type = rs.getString(1);
            String prefix = rs.getString(2);
            String isForeign = rs.getString(3);
            String isPartnership = rs.getString(4);
            String name = rs.getString(5);
            CompanyIndexBean companyIndexBean = new CompanyIndexBean();
            if (type != null) {
                companyIndexBean.setType(type);
            }
            if (prefix != null) {
                companyIndexBean.setPrefix(prefix);
            }
            if ("1".equals(isForeign)) {
                companyIndexBean.setIsForeign(isForeign);
            }
            if ("1".equals(isPartnership)) {
                companyIndexBean.setIsPartnership(isPartnership);
            }
            if (TycUtils.isValidName(name)) {
                companyIndexBean.setName(name);
            }
            return companyIndexBean;
        });
        return res != null ? res : new CompanyIndexBean();
    }

    public String getLegal(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return "";
        }
        String sql = new SQL().SELECT("group_concat(distinct" +
                        " case when legal_rep_type=2 then concat(ifnull(legal_rep_name,''),':',if(legal_rep_name_id!=0,concat(legal_rep_name_id,'-',company_id),''),':',ifnull(legal_rep_human_id,''),':human')" +
                        " when legal_rep_type=1 then concat(ifnull(legal_rep_name,''),':',if(legal_rep_name_id!=0,legal_rep_name_id,''),':company')" +
                        " else '' end SEPARATOR '@@@@@')")
                .FROM("company_legal_person")
                .WHERE("legal_rep_type in (1,2)")
                .WHERE("legal_rep_name_id > 0")
                .WHERE("legal_rep_name is not null")
                .WHERE("legal_rep_name <> ''")
                .WHERE("company_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        String res = companyBase.queryForObject(sql, rs -> rs.getString(1));
        if (res == null) {
            return "";
        } else {
            return Arrays.stream(res.split("@@@@@"))
                    .filter(e -> e.matches(".*?:\\d+:company") || e.matches(".*?:\\d+-\\d+:.{17,}:human"))
                    .collect(Collectors.joining(","));
        }
    }

    public String getIsListed(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return "0";
        }
        String sql = new SQL().SELECT("1")
                .FROM("company_bond_plates join company_graph on company_bond_plates.company_id = company_graph.company_id")
                .WHERE("company_bond_plates.deleted = 0")
                .WHERE("company_graph.deleted = 0")
                .WHERE("company_bond_plates.listing_status not in('暂停上市','IPO终止','退市整理','终止上市')")
                .WHERE("company_graph.graph_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        String res = prism116.queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "0";
    }

    public String listedController(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return "";
        }
        String sql = new SQL().SELECT("group_concat(distinct" +
                        " case when controller_type=0 then concat(controller_name,':',controller_gid,'-',graph_id,':',controller_pid,':human')" +
                        " when controller_type=1 then concat(controller_name,':',if(controller_gid=0,'',controller_gid),':company')" +
                        " else '' end SEPARATOR '@@@@@')")
                .FROM("stock_actual_controller")
                .WHERE("is_deleted = 0")
                .WHERE("controller_type in (0,1)")
                .WHERE("controller_gid > 0")
                .WHERE("controller_name is not null")
                .WHERE("controller_name <> ''")
                .WHERE("graph_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        String res = dataListedCompany.queryForObject(sql, rs -> rs.getString(1));
        if (res == null) {
            return "";
        } else {
            return Arrays.stream(res.split("@@@@@"))
                    .filter(e -> e.matches(".*?:\\d+:company") || e.matches(".*?:\\d+-\\d+:.{17,}:human"))
                    .collect(Collectors.joining(","));
        }
    }

    public String getCompanyHumanPosition(String companyGid, String humanGid) {
        if (!TycUtils.isUnsignedId(companyGid) || !TycUtils.isUnsignedId(humanGid)) {
            return "";
        }
        String sql = new SQL().SELECT("group_concat(distinct personnel_position)")
                .FROM("personnel_employment_history")
                .WHERE("is_deleted = 0")
                .WHERE("is_history = 0")
                .WHERE("company_id = " + SqlUtils.formatValue(companyGid))
                .WHERE("personnel_name_id = " + SqlUtils.formatValue(humanGid))
                .toString();
        String res = humanBase.queryForObject(sql, rs -> rs.getString(1));
        return TycUtils.isValidName(res) ? res : "";
    }

    public String getHumanName(String humanPid) {
        if (!TycUtils.isTycUniqueEntityId(humanPid) || humanPid.length() < 17) {
            return "";
        }
        String sql = new SQL().SELECT("human_name")
                .FROM("human")
                .WHERE("human_id = " + SqlUtils.formatValue(humanPid))
                .toString();
        String res = humanBase.queryForObject(sql, rs -> rs.getString(1));
        return TycUtils.isValidName(res) ? res : "";
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public final static class InvestmentRelationBean {
        private String shareholderGid;
        private String shareholderPid;
        private String equityRatio;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public final static class CompanyIndexBean {
        private String type = "";
        private String prefix = "";
        private String isForeign = "0";
        private String isPartnership = "0";
        private String name = "";
    }
}
