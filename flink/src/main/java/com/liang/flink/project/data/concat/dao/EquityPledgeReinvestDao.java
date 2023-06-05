package com.liang.flink.project.data.concat.dao;

import com.liang.common.service.database.template.JdbcTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;


public class EquityPledgeReinvestDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("operatingRisk");
    private final JdbcTemplate jdbcTemplateCompanyBase = new JdbcTemplate("companyBase");
    private final EquityPledgeReinvestSql sqlHolder = new EquityPledgeReinvestSql();

    public Long queryTotalEquity(String companyId, boolean isHistory) {
        String sql = sqlHolder.totalEquitySql(companyId, isHistory);
        String queryResult = jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
        if (!(StringUtils.isNumeric(queryResult) && !"0".equals(queryResult))) {
            return null;
        }
        return Long.parseLong(queryResult) * 10000L;
        /*BigDecimal res = new BigDecimal(queryResult);
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
        }*/
    }

    public Tuple3<String, String, String> queryMaxTargetCompany(String companyId, boolean isHistory) {
        String sql = sqlHolder.maxTargetCompanySql(companyId, isHistory);
        Tuple3<String, String, String> result = jdbcTemplate.queryForObject(sql, rs -> {
            String type = rs.getString(1);
            String id = rs.getString(2);
            String name = jdbcTemplateCompanyBase.queryForObject(String.format("select company_name from company_index where company_id = '%s'", id), rs2 -> rs2.getString(1));
            return name != null ? Tuple3.of(type, id, name) : Tuple3.of(null, null, null);
        });
        return result != null ? result : Tuple3.of(null, null, null);
    }

    public Tuple3<String, String, String> queryMaxPledgor(String companyId, boolean isHistory) {
        String sql = sqlHolder.maxPledgorSql(companyId, isHistory);
        Tuple3<String, String, String> result = jdbcTemplate.queryForObject(sql, rs -> Tuple3.of(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3)
        ));
        return result != null ? result : Tuple3.of(null, null, null);
    }
}
