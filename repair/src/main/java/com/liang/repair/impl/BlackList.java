package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;

public class BlackList extends ConfigHolder {
    public static void main(String[] args) {
        new JdbcTemplate("430.graph_data").update("delete from company_equity_relation_details where company_id_invested = 353803157");
        // 深圳厚生新金融控股有限公司 华猛
        new JdbcTemplate("457.prism_shareholder_path")
                .update("delete from ratio_path_company where shareholder_id = 'A01UPM509B80GHTHR'");
        // 深圳厚生新金融控股有限公司 华猛
        new JdbcTemplate("463.bdp_equity")
                .update(
                        "delete from entity_controller_details_new where tyc_unique_entity_id = 'A01UPM509B80GHTHR'",
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
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = '4007VMD00G8D0BNHB' ");// 合作伙伴
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'L0S80MQ093LDVD48F' ");// 合作伙伴
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'Y0CENMJ00GG2SN24Y' ");// 合作伙伴
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'H0MEMMQ0A9G2SN24Q' ");// 合作伙伴
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'H0M6MMQ0A9K2BF23Q' ");// 合作伙伴
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = '30AQAM0020U7VSL5A' ");// 合作伙伴
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = '1017NMG0128P0BNHV' ");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'C00D2MS027C197JEN' ");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'T0142MQ024R6FVJLD' and (company_name = '天津市庆军德医疗器械有限公司' or partner_human_name = '王晶')");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = '50E4CMQ01YRBFVJLE'");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'V0134MP09J7AUV0Y5'");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from investment_relation where company_id_invested = 3402590853");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from investment_relation where company_id_invested = 2357995321");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from investment_relation where company_id_invested = 5049963738");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from investment_relation where company_id_invested = 6702272048");
        // 数字安徽有限责任公司 实控人
        new JdbcTemplate("457.prism_shareholder_path")
                .update("delete from investment_relation where company_id_invested = 5565229730 and company_entity_inlink like '%:1186041:company%'");
        new JdbcTemplate("457.prism_shareholder_path")
                .update("delete from investment_relation where company_id_invested = 6808965");
        // 删除老板所有数据
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'F01Y09N0980P52BV3'");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from ratio_path_company where shareholder_id = 'F01Y09N0980P52BV3'");
        new JdbcTemplate("463.bdp_equity").update(
                "delete from entity_controller_details_new where tyc_unique_entity_id = 'F01Y09N0980P52BV3'",
                "delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary = 'F01Y09N0980P52BV3'",
                "delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type = 'F01Y09N0980P52BV3'");
        // 删除老板所有数据
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'Q01589P02D8FNTBU4'");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from ratio_path_company where shareholder_id = 'Q01589P02D8FNTBU4'");
        new JdbcTemplate("463.bdp_equity").update(
                "delete from entity_controller_details_new where tyc_unique_entity_id = 'Q01589P02D8FNTBU4'",
                "delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary = 'Q01589P02D8FNTBU4'",
                "delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type = 'Q01589P02D8FNTBU4'");
        // 删除老板所有数据
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'R01P4MP09TH6V7296'");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from ratio_path_company where shareholder_id = 'R01P4MP09TH6V7296'");
        new JdbcTemplate("463.bdp_equity").update(
                "delete from entity_controller_details_new where tyc_unique_entity_id = 'R01P4MP09TH6V7296'",
                "delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary = 'R01P4MP09TH6V7296'",
                "delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type = 'R01P4MP09TH6V7296'");
        // 删除老板所有数据
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'R01R4MP09T361FK7S'");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from ratio_path_company where shareholder_id = 'R01R4MP09T361FK7S'");
        new JdbcTemplate("463.bdp_equity").update(
                "delete from entity_controller_details_new where tyc_unique_entity_id = 'R01R4MP09T361FK7S'",
                "delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary = 'R01R4MP09T361FK7S'",
                "delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type = 'R01R4MP09T361FK7S'");
        // 删除老板所有数据
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = '90LLJM300EKUPFJ6G'");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from ratio_path_company where shareholder_id = '90LLJM300EKUPFJ6G'");
        new JdbcTemplate("463.bdp_equity").update(
                "delete from entity_controller_details_new where tyc_unique_entity_id = '90LLJM300EKUPFJ6G'",
                "delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary = '90LLJM300EKUPFJ6G'",
                "delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type = '90LLJM300EKUPFJ6G'");
        // 删除老板所有数据
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = '20TVMMN02E33YF576'");
        new JdbcTemplate("457.prism_shareholder_path").update("delete from ratio_path_company where shareholder_id = '20TVMMN02E33YF576'");
        new JdbcTemplate("463.bdp_equity").update(
                "delete from entity_controller_details_new where tyc_unique_entity_id = '20TVMMN02E33YF576'",
                "delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary = '20TVMMN02E33YF576'",
                "delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type = '20TVMMN02E33YF576'");
        //
        new JdbcTemplate("457.prism_shareholder_path").update("delete from investment_relation where company_id_invested in (1017,1065,1101,1172,1258,1301,1307,1347,1418,1617)");
        // 删除老板所有数据
        deleteBossAll("20TVMMN02E33YF576");
        deleteBossAll("10LSG9902Q1L1V561");
        deleteBossAll("Z0165AK02P7T91YRA");
        deleteBossAll("F0L2CM000ARSLHNRY");
        deleteBossAll("K0MA4MG00PARYHCLT");
        deleteBossAll("R00Q290008HDN9B2G");
        deleteBossAll("L01AT9H0986PS2D41");
        deleteBossAll("Z003DM00M5SS7VKBM");
        deleteBossAll("N09HN960217QT9BBA");
        deleteBossAll("90AYYMD01ZMJHYKEF");
        deleteBossAll("C0A3QMQ02M83DVTVZ");
        deleteBossAll("8062G1F01V52K6ZYL");
        deleteBossAll("901J5MY00K5GNVK1G");
        deleteBossAll("Y02PT9J02Q83M1CAF");
        deleteBossAll("00NBU9H00F11A1JZR");
        deleteBossAll("20T969Z00B3MR23DL");
        deleteBossAll("908GUM200SZZNVCGT");
        // 删除合作伙伴
        deletePartner("40TN89C001CR0MVL6");
        deletePartner("B0J5BMG09A081DDA7");
        deletePartner("603UAMV09HHJRFBVP");
        deletePartner("P0PQH9G09S9KN2J0R");
        deletePartner("Y0M81MU02YNG1DYGL");
        deletePartner("20ALJMQ096V0YBAZ2");
        deletePartner("20ALJMQ096V0YBAZ2");
        deletePartner("J00LMMR0ABV5YBAZL");
        deletePartner("C07V09V0TE6RTTU1B");
        deletePartner("U0PVH9A09F67TTU12");
        deletePartner("401F1MV02P311SBGP");
        deletePartner("B0AF4M902T3C1SBGN");
        deletePartner("805GLM500HKK7F545");
        deletePartner("7018SMY0MAZ35FB4N");
        deletePartner("P0NGPMM0T5KC7F544");
        deletePartner("603UAMV09HHJRFBVP");
        deletePartner("80L3PMH020NEYDJ1A");
        deletePartner("H01HT99091HHL0Q2V");
        deletePartner("80MH79E0ATH4L0Q28");
        deletePartner("A01LSMK0TA8VSBZUJ");
        deletePartner("00Z0QMF0152Q9HQZS");
        deletePartner("60937MC00LF5DFLNS");
        deletePartner("S0ANCMY09Q106BHGD");
        deletePartner("80N3BMN0TJFYDFLNZ");
        deletePartner("90ME6M1005E5JB281");
        deletePartner("V0FGQMK09E43HFLD7");
        deletePartner("Z0S3RT30TN33DSSNP");
        deletePartner("M0A04MP02N3A9HG84");
        deletePartner("90G0VME0MQAAKHZEC");
        deletePartner("Q0A69MA02KQRPVEF5");
        deletePartner("G0E6AM300ZQBPVEFM");
        deletePartner("8019FM202G2GC76H4");
        deletePartner("B0ELYMS00JAGTB38V");
        deletePartner("R093VMQ0T8UPJV81Z");
        deletePartner("F0FB89309DYFL1NE5");
        deletePartner("10PEMMF09AM2AF5E5");
        new JdbcTemplate("116.prism").update("delete from equity_ratio where company_graph_id = 9196033");
        deletePartner("F0FB89309DYFL1NE5");
        deletePartner("M0KHSMM0ABEC6BPF9");
        deletePartner("D08PA9M0A2KKU2ED9");
        deletePartner("504DLMR02F2C5PRR7");
        deletePartner("201JD9M09ERCC2JEY");
        deletePartner("000GAM300MB79VUC8");
        deletePartner("60A4N9A0M7G7B1QKB");
        deletePartner("F0LYD92022TVK98ZK");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'D0B77MY02NP8TBT8R' and company_name not like '%陕西%'");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'E044992098GAB1QKP' and company_name = '佳木斯运隆建筑装饰工程有限公司'");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'C0AE0MQ008NPTFU23' and company_name not like '%郑州新铁%'");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = 'J0E6VML0978V9F8PH' and partner_human_name = '张耀疆'");
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = '10LSG9902Q1L1V561' and company_name not like '%山东山鼎%'");
    }

    /**
     *
     */
    private static void deleteBossAll(String humanPid) {
        deletePartner(humanPid);
        new JdbcTemplate("457.prism_shareholder_path").update("delete from ratio_path_company where shareholder_id = '" + humanPid + "'");
        new JdbcTemplate("463.bdp_equity").update(
                "delete from entity_controller_details where tyc_unique_entity_id = '" + humanPid + "'",
                "delete from entity_beneficiary_details where tyc_unique_entity_id_beneficiary = '" + humanPid + "'",
                "delete from shareholder_identity_type_details where tyc_unique_entity_id_with_shareholder_identity_type = '" + humanPid + "'",
                "delete from entity_controller_details_new where tyc_unique_entity_id = '" + humanPid + "'");
    }

    /**
     *
     */
    private static void deletePartner(String humanPid) {
        new JdbcTemplate("467.company_base").update("delete from cooperation_partner where boss_human_pid = '" + humanPid + "'");
    }
}
