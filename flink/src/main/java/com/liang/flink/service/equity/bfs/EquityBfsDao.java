package com.liang.flink.service.equity.bfs;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.flink.service.equity.bfs.dto.mysql.CompanyEquityRelationDetailsDto;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EquityBfsDao {
    private static final List<String> NOT_ALIVE_TAG_ID_LIST = Arrays.asList("34", "35", "36", "37", "38", "39", "40", "43", "44", "46", "47", "48", "49", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "67", "68");
    private final JdbcTemplate graphData = new JdbcTemplate("430.graph_data");
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate companyBase142 = new JdbcTemplate("142.company_base");
    private final JdbcTemplate humanBase040 = new JdbcTemplate("040.human_base");

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
                .SELECT("equity_ratio")
                .FROM("company_equity_relation_details")
                .WHERE("company_id_invested = " + SqlUtils.formatValue(companyId))
                .WHERE("reference_pt_year = 2024")
                .toString();
        return graphData.queryForList(sql, rs -> {
            String id = rs.getString(1);
            String name = rs.getString(2);
            BigDecimal ratio = new BigDecimal(rs.getString(3));
            return new CompanyEquityRelationDetailsDto(id, name, ratio);
        });
    }

    public boolean isClosed(String companyId) {
        String sql = new SQL()
                .SELECT("1")
                .FROM("bdp_company_profile_tag_details_total")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("profile_tag_id in " + NOT_ALIVE_TAG_ID_LIST.stream().collect(Collectors.joining(",", "(", ")")))
                .toString();
        return companyBase142.queryForObject(sql, rs -> rs.getString(1)) != null;
    }

    public String getUscc(String companyId) {
        String sql = new SQL()
                .SELECT("unified_social_credit_code")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return companyBase435.queryForObject(sql, rs -> rs.getString(1));
    }

    /**
     * human or company
     */
    public Map<String, Object> queryHumanOrCompanyInfo(String id, String shareholderType) {
        String sql = "2".equals(shareholderType) ?
                new SQL()
                        .SELECT("human_name_id", "master_company_id", "human_name")
                        .FROM("human")
                        .WHERE("human_id = " + SqlUtils.formatValue(id))
                        .toString() :
                new SQL()
                        .SELECT("company_id", "company_id", "company_name")
                        .FROM("company_index")
                        .WHERE("company_id = " + SqlUtils.formatValue(id))
                        .toString();
        JdbcTemplate jdbcTemplate = "2".equals(shareholderType) ?
                humanBase040 :
                companyBase435;
        Map<String, Object> columnMap = jdbcTemplate.queryForObject(sql, rs -> new HashMap<String, Object>() {{
            put("name_id", rs.getString(1));
            put("company_id", rs.getString(2));
            put("name", rs.getString(3));
            put("id", id);
        }});
        return columnMap != null ? columnMap : new HashMap<>();
    }
}
