package com.liang.flink.dao;

import com.liang.common.service.database.template.JdbcTemplate;
import org.apache.flink.api.java.tuple.Tuple3;

public class EquityPledgeDetailDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("operatingRisk");
    private final EquityPledgeDetailSql sqlHolder = new EquityPledgeDetailSql();

    public Long queryTotalEquity(String companyId) {
        String sql = sqlHolder.totalEquitySql(companyId);
        Long res = jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
        return res != null && res > 0 ? res : null;
    }

    public Tuple3<String, String, String> queryMaxTargetCompany(String companyId) {
        String sql = sqlHolder.maxTargetCompanySql(companyId);
        Tuple3<String, String, String> res = jdbcTemplate.queryForObject(sql, rs -> Tuple3.of(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3)
        ));
        return res != null ? res : Tuple3.of(null, null, null);
    }

    public Tuple3<String, String, String> queryMaxPledgor(String companyId) {
        String sql = sqlHolder.maxPledgorSql(companyId);
        Tuple3<String, String, String> res = jdbcTemplate.queryForObject(sql, rs -> Tuple3.of(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3)
        ));
        return res != null ? res : Tuple3.of(null, null, null);
    }

}
