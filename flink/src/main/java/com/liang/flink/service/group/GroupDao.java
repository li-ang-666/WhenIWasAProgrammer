package com.liang.flink.service.group;

import cn.hutool.core.util.ObjUtil;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class GroupDao {
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate prismShareholderPath491 = new JdbcTemplate("491.prism_shareholder_path");
    private final JdbcTemplate bdpEquity463 = new JdbcTemplate("463.bdp_equity");
    private final JdbcTemplate listedBase157 = new JdbcTemplate("157.listed_base");

    public Map<String, Object> queryCompanyIndex(String companyId) {
        String sql = new SQL()
                .SELECT("*")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        List<Map<String, Object>> sqls = companyBase435.queryForColumnMaps(sql);
        return sqls.isEmpty() ? new HashMap<>() : sqls.get(0);
    }

    public boolean isCompanyBranch(String companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("company_branch")
                .WHERE("branch_company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("is_deleted = 0")
                .toString();
        return companyBase435.queryForObject(sql, rs -> rs.getString(1)) != null;
    }

    public List<String> queryMaxRatioShareholder(String companyId) {
        String table = "ratio_path_company_new_" + Long.parseLong(companyId) % 100;
        String preSql = new SQL().SELECT("max(investment_ratio_total)")
                .FROM(table)
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("company_id <> shareholder_id")
                .LIMIT(1)
                .toString();
        String sql = new SQL().SELECT("shareholder_id")
                .FROM(table)
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("investment_ratio_total = (" + preSql + ")")
                .WHERE("shareholder_entity_type = 1")
                .WHERE("company_id <> shareholder_id")
                .toString();
        return prismShareholderPath491.queryForList(sql, rs -> rs.getString(1));
    }

    public Long queryControllingSize(String shareholderId) {
        String sql = new SQL().SELECT("count(1)")
                .FROM("entity_controller_details_new")
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(shareholderId))
                .toString();
        Long res = bdpEquity463.queryForObject(sql, rs -> rs.getLong(1));
        return ObjUtil.defaultIfNull(res, 0L);
    }
}
