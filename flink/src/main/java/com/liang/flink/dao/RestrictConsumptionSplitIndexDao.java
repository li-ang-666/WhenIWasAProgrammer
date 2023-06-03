package com.liang.flink.dao;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.flink.service.Calculator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestrictConsumptionSplitIndexDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("judicialRisk");
    private final RestrictConsumptionSplitIndexSql sqlHolder = new RestrictConsumptionSplitIndexSql();

    /**
     * SQL返回格式:
     * 山东永宇经贸有限公司:366128831;山东日金集团有限公司:366129852
     */
    public String queryMostApplicant(String companyId, boolean isHistory) {
        Calculator calculator = new Calculator();
        String sql = sqlHolder.queryMostApplicantSql(companyId, isHistory);
        jdbcTemplate.queryForList(sql, rs -> {
            String concat = rs.getString(1);
            int cnt = rs.getInt(2);
            String[] split = concat.split(";");
            for (String s : split) {
                String[] splitSplit = s.split("、");
                for (String ss : splitSplit) {
                    if (!"其他".equals(ss) && !"其它".equals(ss))
                        calculator.add(ss, cnt);
                }
            }
            return null;
        });
        return calculator.getMaxCountKey();
    }

    /**
     * SQL返回格式:
     * 邱中华:608FJ9S003SA5M9T9:2207410658-409561662:human
     * JINGHE:3220644254:company
     */
    public String queryMostRelatedRestricted(String restrictedId, boolean isHistory) {
        Calculator calculator = new Calculator();
        String sql = sqlHolder.queryMostRelatedRestrictedSql(restrictedId, isHistory);
        jdbcTemplate.queryForList(sql, rs -> {
            String concat = rs.getString(1);
            int cnt = rs.getInt(2);
            String[] split = concat.split(";");
            for (String s : split) {
                String[] splitSplit = s.split("、");
                for (String ss : splitSplit) {
                    if (!"其他".equals(ss) && !"其它".equals(ss))
                        calculator.add(ss, cnt);
                }
            }
            return null;
        });
        return calculator.getMaxCountKey();
    }

    /**
     * SQL返回格式:
     * 邱中华:608FJ9S003SA5M9T9:2207410658-409561662:human
     * JINGHE:3220644254:company
     */
    public String queryMostRestricted(String relatedRestrictedId, boolean isHistory) {
        Calculator calculator = new Calculator();
        String sql = sqlHolder.queryMostRestrictedSql(relatedRestrictedId, isHistory);
        jdbcTemplate.queryForList(sql, rs -> {
            String concat = rs.getString(1);
            int cnt = rs.getInt(2);
            String[] split = concat.split(";");
            for (String s : split) {
                String[] splitSplit = s.split("、");
                for (String ss : splitSplit) {
                    if (!"其他".equals(ss) && !"其它".equals(ss))
                        calculator.add(ss, cnt);
                }
            }
            return null;
        });
        return calculator.getMaxCountKey();
    }
}
