package com.liang.flink.service.equity.bfs;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.flink.service.equity.bfs.dto.CompanyEquityRelationDetailsDto;

import java.math.BigDecimal;
import java.util.List;

public class EquityBfsDao {
    private final JdbcTemplate graphData = new JdbcTemplate("430.graph_data");
    private final JdbcTemplate prismShareholderPath = new JdbcTemplate("457.prism_shareholder_path");
    private final JdbcTemplate companyBase = new JdbcTemplate("435.company_base");
    private final JdbcTemplate humanBase = new JdbcTemplate("040.human_base");
    private final JdbcTemplate listedBase = new JdbcTemplate("157.listed_base");

    public String queryCompanyName(String companyId) {
        String sql = new SQL().SELECT("company_name")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return companyBase.queryForObject(sql, rs -> rs.getString(1));
    }

    public List<CompanyEquityRelationDetailsDto> queryShareholder(String companyId) {
        String sql = new SQL()
                .SELECT("tyc_unique_entity_id_investor")
                .SELECT("tyc_unique_entity_name_investor")
                .SELECT("equity_ratio")
                .FROM("company_equity_relation_details")
                .WHERE("company_id_invested = " + SqlUtils.formatValue(companyId))
                .WHERE("reference_pt_year = 2024")
                .toString();
        return graphData.queryForList(sql,
                rs -> new CompanyEquityRelationDetailsDto(rs.getString(1), rs.getString(2), new BigDecimal(rs.getString(3))));
    }
}
