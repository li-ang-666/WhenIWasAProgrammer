package com.liang.flink.dao;

import com.liang.common.service.database.template.JdbcTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
public class JudicialAssistanceIndexDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("judicialRisk");
    private final JdbcTemplate jdbcTemplateCompany = new JdbcTemplate("companyBase");
    private final JudicialAssistanceIndexSql sqlHolder = new JudicialAssistanceIndexSql();

    /**
     * SQL返回格式:
     * 一个数字
     */
    public String queryTotalFrozenEquity(String companyIdOrEnforcedTargetId, boolean isHistory) {
        String sql = sqlHolder.queryTotalFrozenEquitySql(companyIdOrEnforcedTargetId, isHistory);
        String queryResult = jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
        if (queryResult == null) {
            return null;
        }
        BigDecimal res = new BigDecimal(queryResult);
        if ((res.doubleValue() * 10000L / 100000000L) > 1) {
            return res
                    .multiply(BigDecimal.valueOf(10000L))
                    .divide(BigDecimal.valueOf(100000000L), 4, RoundingMode.HALF_UP)
                    .setScale(4, RoundingMode.HALF_UP)
                    .stripTrailingZeros().toPlainString() + "亿元";
        } else {
            return res
                    .setScale(4, RoundingMode.HALF_UP)
                    .stripTrailingZeros().toPlainString() + "万元";
        }
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
        return Tuple3.of("1", equityFrozenCompanyId, equityFrozenCompanyName);
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
