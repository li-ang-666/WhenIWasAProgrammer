package com.liang.flink.job;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.service.LocalConfigFile;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
@LocalConfigFile("group.yml")
public class GroupJob {
    public static void main(String[] args) {
        EnvironmentFactory.create(args);
        JdbcTemplate graphData430 = new JdbcTemplate("430.graph_data");
        String companyId = "27624827";
        String sql = new SQL().SELECT("*")
                .FROM("company_equity_relation_details")
                .WHERE("shareholder_id = " + SqlUtils.formatValue(companyId))
                .toString();
        List<Map<String, Object>> columnMaps = graphData430.queryForColumnMaps(sql);
        for (Map<String, Object> columnMap : columnMaps) {
            String investedCompanyId = (String) columnMap.get("company_id");
            String equityRatio = (String) columnMap.get("equity_ratio");
            String maxEquityRatioSql = new SQL().SELECT("max(equity_ratio)")
                    .FROM("company_equity_relation_details")
                    .WHERE("company_id = " + SqlUtils.formatValue(investedCompanyId))
                    .toString();
            String maxRatio = graphData430.queryForObject(maxEquityRatioSql, rs -> rs.getString(1));
            if (maxRatio.equals(equityRatio)) {
                System.out.println(investedCompanyId);
            }
        }
    }
}
