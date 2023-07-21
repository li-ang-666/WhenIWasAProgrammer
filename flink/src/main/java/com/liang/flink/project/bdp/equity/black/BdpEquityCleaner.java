package com.liang.flink.project.bdp.equity.black;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class BdpEquityCleaner implements Runnable {
    private final JdbcTemplate prismShareholderPath = new JdbcTemplate("prismShareholderPath");
    private final JdbcTemplate bdpEquity = new JdbcTemplate("bdpEquity");

    @Override
    @SneakyThrows(InterruptedException.class)
    public void run() {
        while (true) {
            String sqlA = new SQL().UPDATE("ratio_path_company")
                    .SET("is_controller = 0").SET("is_controlling_shareholder = 0")
                    .WHERE("company_id = 2338203553").toString();
            String sqlB = new SQL().DELETE_FROM("ratio_path_company")
                    .WHERE("company_id in ('','0')").toString();
            String sqlC = new SQL().DELETE_FROM("ratio_path_company")
                    .WHERE("shareholder_id in ('','0')").toString();
            prismShareholderPath.update(sqlA, sqlB, sqlC);
            String sql1 = new SQL().DELETE_FROM("entity_beneficiary_details")
                    .WHERE("tyc_unique_entity_id in ('','0')").toString();
            String sql2 = new SQL().DELETE_FROM("entity_beneficiary_details")
                    .WHERE("tyc_unique_entity_id_beneficiary in ('','0')").toString();
            String sql3 = new SQL().DELETE_FROM("entity_controller_details")
                    .WHERE("tyc_unique_entity_id in ('','0')").toString();
            String sql4 = new SQL().DELETE_FROM("entity_controller_details")
                    .WHERE("company_id_controlled in ('','0')").toString();
            String sql5 = new SQL().DELETE_FROM("shareholder_identity_type_details")
                    .WHERE("tyc_unique_entity_id in ('','0')").toString();
            String sql6 = new SQL().DELETE_FROM("shareholder_identity_type_details")
                    .WHERE("tyc_unique_entity_id_with_shareholder_identity_type in ('','0')").toString();
            bdpEquity.update(sql1, sql2, sql3, sql4, sql5, sql6);
            log.info("处理黑名单...");
            TimeUnit.MINUTES.sleep(3);
        }
    }
}
