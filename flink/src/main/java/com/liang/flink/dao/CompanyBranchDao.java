package com.liang.flink.dao;


import com.liang.common.service.database.template.JdbcTemplate;

public class CompanyBranchDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("companyBase");
    private final CompanyBranchSql sqlHolder = new CompanyBranchSql();

    public Long queryTotalBranch(String companyId) {
        String sql = sqlHolder.queryTotalBranchSql(companyId);
        return jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
    }

    public Long queryTotalCanceledBranch(String companyId) {
        String sql = sqlHolder.queryTotalCanceledBranchSql(companyId);
        return jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
    }

    public String queryMostYear(String companyId) {
        String sql = sqlHolder.queryMostYearSql(companyId);
        return jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
    }

    public String queryMostArea(String companyId) {
        String sql = sqlHolder.queryMostAreaSql(companyId);
        return jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
    }
}
