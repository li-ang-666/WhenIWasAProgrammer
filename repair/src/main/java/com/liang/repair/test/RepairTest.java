package com.liang.repair.test;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        JdbcTemplate graphData430 = new JdbcTemplate("430.graph_data");
        String sql = new SQL().SELECT("*")
                .FROM("company_equity_relation_details")
                .WHERE("company_id = 1163921791")
                .toString();
        List<Map<String, Object>> columnMaps = graphData430.queryForColumnMaps(sql);
        System.out.println(columnMaps);
    }
}
