package com.liang.flink.project.dim.count.dao;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ApolloUtils;

import static com.liang.common.util.SqlUtils.formatValue;

public class ShareholderTagDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("457.prism_shareholder_path");

    public Integer queryHasController(Object companyId) {
        String sql = String.format(ApolloUtils.get("queryHasController"), formatValue(companyId));
        return jdbcTemplate.queryForObject(sql, rs -> rs.getInt(1));
    }

    public Integer queryHasBeneficiary(Object companyId) {
        String sql = String.format(ApolloUtils.get("queryHasBeneficiary"), formatValue(companyId));
        return jdbcTemplate.queryForObject(sql, rs -> rs.getInt(1));
    }

    public Integer queryNumControlAbility(Object shareholderId) {
        String sql = String.format(ApolloUtils.get("queryNumControlAbility"), formatValue(shareholderId));
        return jdbcTemplate.queryForObject(sql, rs -> rs.getInt(1));
    }

    public Integer queryNumBenefitAbility(Object shareholderId) {
        String sql = String.format(ApolloUtils.get("queryNumBenefitAbility"), formatValue(shareholderId));
        return jdbcTemplate.queryForObject(sql, rs -> rs.getInt(1));
    }
}
