package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;

public class BlackList extends ConfigHolder {
    public static void main(String[] args) {
        // 深圳厚生新金融控股有限公司 华猛
        new JdbcTemplate("457.prism_shareholder_path")
                .update("delete from ratio_path_company where shareholder_id = 'A01UPM509B80GHTHR'");
        // 深圳厚生新金融控股有限公司 华猛
        new JdbcTemplate("463.bdp_equity")
                .update(
                        "delete from entity_controller_details where tyc_unique_entity_id = 'A01UPM509B80GHTHR'",
                        "delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary = 'A01UPM509B80GHTHR'",
                        "delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type = 'A01UPM509B80GHTHR'"
                );
        // 深圳厚生新金融控股有限公司 实控人
        new JdbcTemplate("457.prism_shareholder_path")
                .update("delete from investment_relation where company_id_invested = 2354576202");
        // 支付宝 实控人
        new HbaseTemplate("hbaseSink")
                .update(new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, "64786241").put("has_controller", null));
        // 合作伙伴
        new JdbcTemplate("467.company_base")
                .update("delete from cooperation_partner where boss_human_pid = '90Y6ZMF09JQEPVEF8' and partner_human_name in ('房思菊')");
        // 合作伙伴
        new JdbcTemplate("467.company_base")
                .update("delete from cooperation_partner where boss_human_pid = 'B0LM3MN026619B5PV' ");
        // 合作伙伴
        new JdbcTemplate("467.company_base")
                .update("delete from cooperation_partner where boss_human_pid = '604CHMA09HHC3DQ2V' ");// 合作伙伴
        new JdbcTemplate("467.company_base")
                .update("delete from cooperation_partner where boss_human_pid = '4007VMD00G8D0BNHB' ");// 合作伙伴
        new JdbcTemplate("467.company_base")
                .update("delete from cooperation_partner where boss_human_pid = '1017NMG0128P0BNHV' ");
        // 数字安徽有限责任公司 实控人
        new JdbcTemplate("457.prism_shareholder_path")
                .update("delete from investment_relation where company_id_invested = 5565229730 and company_entity_inlink like '%:1186041:company%'");
        new JdbcTemplate("457.prism_shareholder_path")
                .update("delete from investment_relation where company_id_invested = 6808965");
    }
}
