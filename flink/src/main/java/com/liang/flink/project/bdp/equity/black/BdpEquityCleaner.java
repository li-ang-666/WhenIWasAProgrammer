package com.liang.flink.project.bdp.equity.black;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class BdpEquityCleaner implements Runnable {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("prismShareholderPath");

    /**
     * delete from entity_beneficiary_details where tyc_unique_entity_id in ('','0');
     * delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary in ('','0');
     * delete from entity_controller_details where tyc_unique_entity_id in ('','0');
     * delete from entity_controller_details where company_id_controlled in ('','0');
     * delete from shareholder_identity_type_details where tyc_unique_entity_id in ('','0');
     * delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type in ('','0');
     */
    @Override
    @SneakyThrows(InterruptedException.class)
    public void run() {
        while (true) {
            String sql0 = new SQL().UPDATE("ratio_path_company")
                    .SET("is_controller = 0").SET("is_controlling_shareholder = 0")
                    .WHERE("company_id = 2338203553").toString();
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
            jdbcTemplate.update(sql0, sql1, sql2, sql3, sql4, sql5, sql6);
            log.info("处理黑名单...");
            TimeUnit.MINUTES.sleep(3);
        }
    }
}
