package com.liang.flink.project.data.concat.dao;

import com.liang.common.service.database.template.JdbcTemplate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.concurrent.TimeUnit;


@Slf4j
public class EquityPledgeReinvestDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("operatingRisk");
    private final JdbcTemplate jdbcTemplateCompanyBase = new JdbcTemplate("companyBase");
    private final EquityPledgeReinvestSql sqlHolder = new EquityPledgeReinvestSql();

    public Long queryTotalEquity(String companyId, boolean isHistory) {
        String sql = sqlHolder.totalEquitySql(companyId, isHistory);
        Long queryResult = jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
        if (queryResult == null) {
            return queryResult;
        }
        return 10000L * queryResult;
    }

    @SneakyThrows
    public Tuple3<String, String, String> queryMaxTargetCompany(String companyId, boolean isHistory) {
        String sql = sqlHolder.maxTargetCompanySql(companyId, isHistory);
        Tuple3<String, String, String> result = jdbcTemplate.queryForObject(sql, rs -> {
            String type = rs.getString(1);
            String id = rs.getString(2);
            String mainId = rs.getString(3);
            String name = jdbcTemplateCompanyBase.queryForObject(String.format("select company_name from company_index where company_id = '%s'", id), rs2 -> rs2.getString(1));
            if (name == null) {
                TimeUnit.SECONDS.sleep(1);
                name = jdbcTemplate.queryForObject("select equity_pledge_target_company_name from equity_pledge_reinvest_index where main_id = " + mainId, rs3 -> rs3.getString(1));
            }
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
