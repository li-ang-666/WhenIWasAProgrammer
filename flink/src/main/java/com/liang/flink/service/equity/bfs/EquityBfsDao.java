package com.liang.flink.service.equity.bfs;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.flink.service.equity.bfs.dto.mysql.CompanyEquityRelationDetailsDto;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EquityBfsDao {
    private static final List<String> ALIVE_TAG_ID_LIST = Arrays.asList("30", "31", "32", "33", "41", "42", "45", "50", "62", "63", "64", "65", "66");
    private final JdbcTemplate graphData = new JdbcTemplate("430.graph_data");
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate companyBase142 = new JdbcTemplate("142.company_base");

    public String queryCompanyName(String companyId) {
        String sql = new SQL().SELECT("company_name")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return companyBase435.queryForObject(sql, rs -> rs.getString(1));
    }

    public List<CompanyEquityRelationDetailsDto> queryShareholder(String companyId) {
        String sql = new SQL()
                .SELECT("tyc_unique_entity_id_investor")
                .SELECT("tyc_unique_entity_name_investor")
                .SELECT("company_id_investor")
                .SELECT("equity_ratio")
                .FROM("company_equity_relation_details")
                .WHERE("company_id_invested = " + SqlUtils.formatValue(companyId))
                .WHERE("reference_pt_year = 2024")
                .toString();
        return graphData.queryForList(sql, rs -> {
            String id = rs.getString(1);
            String name = rs.getString(2);
            String nameId = rs.getString(3);
            BigDecimal ratio = new BigDecimal(rs.getString(4));
            return new CompanyEquityRelationDetailsDto(id, name, nameId, ratio);
        });
    }

    public boolean isAlive(String companyId) {
        String sql = new SQL()
                .SELECT("1")
                .FROM("bdp_company_profile_tag_details_total")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("profile_tag_id in " + ALIVE_TAG_ID_LIST.stream().collect(Collectors.joining(",", "(", ")")))
                .toString();
        return companyBase142.queryForObject(sql, rs -> rs.getString(1)) != null;
    }

    public boolean is001(String companyId) {
        String sql = new SQL()
                .SELECT("unified_social_credit_code")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        String uscc = companyBase435.queryForObject(sql, rs -> rs.getString(1));
        return uscc != null && uscc.startsWith("11");
    }
}
