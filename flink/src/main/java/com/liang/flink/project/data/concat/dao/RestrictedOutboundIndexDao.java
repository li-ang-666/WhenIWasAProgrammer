package com.liang.flink.project.data.concat.dao;


import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.flink.project.data.concat.service.Sorter;

public class RestrictedOutboundIndexDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("judicialRisk");
    private final RestrictedOutboundIndexSql sqlHolder = new RestrictedOutboundIndexSql();

    /**
     * SQL返回格式:
     * 湖州合顺不锈钢管有限公司:1318721194:company;湖州南浔合顺不锈钢管厂:2952669892:company
     */
    public String queryMostApplicant(String companyId) {
        Sorter sorter = new Sorter();
        String sql = sqlHolder.queryMostApplicantSql(companyId);
        jdbcTemplate.queryForList(sql, rs -> {
            String concat = rs.getString(1);
            int cnt = rs.getInt(2);
            String[] split = concat.split(";");
            for (String s : split) {
                String[] splitSplit = s.split("、");
                for (String ss : splitSplit) {
                    if (!"其他".equals(ss) && !"其它".equals(ss))
                        sorter.add(ss, cnt);
                }
            }
            return null;
        });
        return sorter.getMaxCountKey();
    }

    /**
     * SQL返回格式:
     * 韩计强,夏兆利,许晓翔,丁琴芳
     */
    public String queryMostRestricted(String companyId) {
        Sorter sorter = new Sorter();
        String sql = sqlHolder.queryMostRestrictedSql(companyId);
        jdbcTemplate.queryForList(sql, rs -> {
            String concat = rs.getString(1);
            int cnt = rs.getInt(2);
            String[] split = concat.split(";");
            for (String s : split) {
                String[] splitSplit = s.split("、");
                for (String ss : splitSplit) {
                    if (!"其他".equals(ss) && !"其它".equals(ss))
                        sorter.add(ss, cnt);
                }
            }
            return null;
        });
        return sorter.getMaxCountKey();
    }
}
