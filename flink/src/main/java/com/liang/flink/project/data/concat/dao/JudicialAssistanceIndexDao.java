package com.liang.flink.project.data.concat.dao;

import com.liang.common.service.database.template.JdbcTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;

@Slf4j
public class JudicialAssistanceIndexDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("judicialRisk");
    private final JdbcTemplate jdbcTemplateCompany = new JdbcTemplate("companyBase");
    private final JudicialAssistanceIndexSql sqlHolder = new JudicialAssistanceIndexSql();

    /**
     * SQL返回格式:
     * 一个数字
     */
    public Long queryTotalFrozenEquity(String companyIdOrEnforcedTargetId, boolean isHistory) {
        String sql = sqlHolder.queryTotalFrozenEquitySql(companyIdOrEnforcedTargetId, isHistory);
        Long queryResult = jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
        if (queryResult == null) {
            return queryResult;
        }
        return 10000L * queryResult;
    }

    /**
     * SQL返回格式:
     * 一个数字
     */
    public Tuple3<String, String, String> queryMostEquityFrozenCompany(String enforcedTargetId, boolean isHistory) {
        String sql = sqlHolder.queryMostEquityFrozenCompanySql(enforcedTargetId, isHistory);
        String equityFrozenCompanyId = jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
        String equityFrozenCompanyName = jdbcTemplateCompany.queryForObject(
                String.format("select company_name from company_index where company_id = %s", equityFrozenCompanyId),
                rs -> rs.getString(1)
        );
        return Tuple3.of(equityFrozenCompanyName == null ? null : "1",
                equityFrozenCompanyId,
                equityFrozenCompanyName);
    }

    /**
     * SQL返回格式:
     * Tuple3(type, id, name)
     */
    public Tuple3<String, String, String> queryMostEquityFrozenEnforcedTarget(String companyId, boolean isHistory) {
        String sql = sqlHolder.queryMostEquityFrozenEnforcedTargetSql(companyId, isHistory);
        Tuple3<String, String, String> equityFrozenEnforcedTarget = jdbcTemplate.queryForObject(sql, rs -> Tuple3.of(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3)
        ));
        return equityFrozenEnforcedTarget != null ? equityFrozenEnforcedTarget : Tuple3.of(null, null, null);
    }
}
