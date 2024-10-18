package com.liang.flink.project.group.bfs;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class GroupBfsDao {
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate graphData430 = new JdbcTemplate("430.graph_data");

    public Map<String, Object> queryCompanyIndex(String companyId) {
        String sql = new SQL().SELECT("*")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return companyBase435.queryForColumnMap(sql);
    }

    public boolean queryHasShareholder(String companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("company_equity_relation_details")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return graphData430.queryForObject(sql, rs -> rs.getString(1)) != null;
    }
}
