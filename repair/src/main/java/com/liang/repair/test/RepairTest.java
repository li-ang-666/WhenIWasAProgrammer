package com.liang.repair.test;

import cn.hutool.core.io.IoUtil;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        JdbcTemplate prism116 = new JdbcTemplate("116.prism");
        JdbcTemplate graphData430 = new JdbcTemplate("430.graph_data");
        prism116.enableCache();
        graphData430.enableCache();
        InputStream resourceAsStream = RepairTest.class.getClassLoader().getResourceAsStream("wrong-company-ids.txt");
        String[] companyIds = IoUtil.readUtf8(resourceAsStream).split("\n");
        for (String companyId : companyIds) {
            if (!TycUtils.isUnsignedId(companyId)) {
                continue;
            }
            String deleteSql = new SQL().DELETE_FROM("company_equity_relation_details")
                    .WHERE("company_id_invested = " + SqlUtils.formatValue(companyId))
                    .toString();
            graphData430.update(deleteSql);
        }
        graphData430.flush();
        for (String companyId : companyIds) {
            if (!TycUtils.isUnsignedId(companyId)) {
                continue;
            }
            String updateSql = new SQL().UPDATE("equity_ratio")
                    .SET("update_time = now()")
                    .WHERE("company_graph_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            prism116.update(updateSql);
        }
        prism116.flush();
    }
}
