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
        deleteBossAll("A0AVGMJ09T5M4V575");
        deleteBossAll("60LTY91020ZN42PBJ");
        deleteBossAll("J0TE7MD00B3LBFZD5");
        deleteBossAll("J0TE7MD00B3LBFZD5");
        deleteBossAll("C0AS9MY09ZGA7DS9S");
        deleteBossAll("C0AS9MY09ZGA7DS9S");
        deleteBossAll("F00359M025JMD12KM");
        deleteBossAll("5004GM300HNM8Y4JY");
        deleteBossAll("R01QY9G090C7122ZZ");
        deleteBossAll("500QD9G00AC1122ZU");
        deleteBossAll("L0QNUML00TNSDBLF9");
        deleteBossAll("70LJQMK02T8UEVGDB");
        deleteBossAll("T0GC9M80M84KQSVBV");
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
        deleteBossAll("E0123MT02JVL74FAJ");
        deleteBossAll("901J5MY00K5GNVK1G");
        deleteBossAll("Y02PT9J02Q83M1CAF");
        deleteBossAll("00NBU9H00F11A1JZR");
        deleteBossAll("20T969Z00B3MR23DL");
        deleteBossAll("908GUM200SZZNVCGT");
        deleteBossAll("P004HMJ00TGTBYKP6");
        deleteBossAll("J01HR9P09NDU79N8L");
        deleteBossAll("E0TJ0MR02R2TCBSVC");
        deleteBossAll("10AT7MN0M204PHY15");
        deleteBossAll("90FY0MH094PQJ73LV");
        deleteBossAll("30T6ZM602ND0CBCKL");
        deleteBossAll("G0QVQMT01B0CY75A9");
        deleteBossAll("10TNMM602K1GSN032");
        deleteBossAll("40T7JMF09FNPHSB72");
        deleteBossAll("406VTML003VVNF52B");
        deleteBossAll("H0HKFM000YDDFF4VZ");
        deleteBossAll("90071MZ0216M1BGBA");
        deleteBossAll("B0A75ME09BZMJVSZN");
        // 删除合作伙伴
        deletePartner("40TN89C001CR0MVL6");
        deletePartner("U0L6TM70254L9F22R");
        deletePartner("006Y8MG0AR6MCDRBF");
        deletePartner("M018B9N09U8GJMQEB");
        deletePartner("10NHVMV00N5KSP4S7");
        deletePartner("408CGM80ACU1YBT6J");
        deletePartner("D01Q1MM09LMBZVUSS");
        deletePartner("J0A12MA09K70BPP2K");
        deletePartner("E0T11MP09TRP1HGJQ");
        deletePartner("00ZMBMR0AA973DCZF");
        deletePartner("00Z9BMR0AAA7Y46MR");
        deletePartner("00Z9BMR0AAJ70VTVM");
        deletePartner("R0TF6MU00YUJ6V4D5");
        deletePartner("K0YQPML010TFKFYZC");
        deletePartner("M0NECMY0TPFRLF769");
        deletePartner("B0J5BMG09A081DDA7");
        deletePartner("603UAMV09HHJRFBVP");
        deletePartner("P0PQH9G09S9KN2J0R");
        deletePartner("Y0M81MU02YNG1DYGL");
        deletePartner("20ALJMQ096V0YBAZ2");
        deletePartner("20ALJMQ096V0YBAZ2");
        deletePartner("J00LMMR0ABV5YBAZL");
        deletePartner("904DRM402ZHLPD6UT");
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
        deletePartner("50M9BMQ02YY4H4QZT");
        deletePartner("60937MC00LF5DFLNS");
        deletePartner("S0ANCMY09Q106BHGD");
        deletePartner("80N3BMN0TJFYDFLNZ");
        deletePartner("90ME6M1005E5JB281");
        deletePartner("V0FGQMK09E43HFLD7");
        deletePartner("Z0S3RT30TN33DSSNP");
        deletePartner("M0A04MP02N3A9HG84");
        deletePartner("90G0VME0MQAAKHZEC");
        deletePartner("H045R2P023S161FH7");
        deletePartner("Q0A69MA02KQRPVEF5");
        deletePartner("G0E6AM300ZQBPVEFM");
        deletePartner("8019FM202G2GC76H4");
        deletePartner("B0ELYMS00JAGTB38V");
        deletePartner("R093VMQ0T8UPJV81Z");
        deletePartner("F0FB89309DYFL1NE5");
        deletePartner("10PEMMF09AM2AF5E5");
        deletePartner("P052EM401UE0NB7YE");
        new JdbcTemplate("116.prism").update("delete from equity_ratio where company_graph_id = 9196033");
        deletePartner("F0FB89309DYFL1NE5");
        deletePartner("M0KHSMM0ABEC6BPF9");
        deletePartner("D08PA9M0A2KKU2ED9");
        deletePartner("504DLMR02F2C5PRR7");
        deletePartner("201JD9M09ERCC2JEY");
        deletePartner("000GAM300MB79VUC8");
        deletePartner("60A4N9A0M7G7B1QKB");
        deletePartner("30TGVMH0218TNB9NY");
        deletePartner("30YMTM4092GHRDRL2");
        deletePartner("40LBAM30203HPV21S");
        deletePartner("F0LYD92022TVK98ZK");
        deletePartner("F0ER49V0M8N4UMKZZ");
        deletePartner("P0DCMML01FZ2CBY55");
        deletePartner("A0421MC008H2SBD88");
        deletePartner("J09U52N02TNUY9KT7");
        deletePartner("J09B5MN02T3UPV21D");
        deletePartner("L09B7MY0213UPV21P");
        deletePartner("K0YEPML010FFLF76M");
        deletePartner("K0YQPML010TFKFYZC");
        deletePartner("K01BB9K0249YSTV6F");
        deletePartner("50JVD9209TKHC9F9T");
        deletePartner("U0ANKMD02H6YRYVLH");
        deletePartner("90T1Y9E02YCHRTVHB");
        deletePartner("A00M89U02C6LA26HU");
        deletePartner("R0VKVMH09TD23Y04T");
        deletePartner("U0UHYMQ0MDS1PV1SF");
        deletePartner("10AU29D09DRM72A8F");
        deletePartner("80G4C900M8ZS802BH");
        deletePartner("20RHFMH0AAES0V5RD");
        deletePartner("300HYM100FZ05VGHR");
        deletePartner("H06A5M000BNLFD6SE");
        deletePartner("A0AK696093T9DM6LN");
        deletePartner("M0MR3MJ0274ZTB24J");
        deletePartner("90071MZ0216M1BGBA");
        deletePartner("F0AKTMD02SFA8VT9L");
        deletePartner("M0ME3MJ027JZ3DDT6");
        deletePartner("V0EHTM40M6B3CPGDR");
        deletePartner("U0AL89N0954UF2032");
        deletePartner("B09T9950298DJAT0D");
        deletePartner("B09T9950298DJAT0D");
        deletePartner("809N6M902KELSVUTJ");
        deletePartner("T00LQMN00RUU0BAR1");
        deletePartner("J0A12MA09K70BPP2K");
        deleteBossAll("E0L2UM402ZUBU7Y9K");
        deleteBossAll("50S3LM2097MCUFVAK");
        deleteBossAll("10TNMM602K1GSN032");
        deleteBossAll("P0SH6MA09BZDFVFR3");
        deleteBossAll("40HE2M900H7P4FK23");
        deleteBossAll("M0CZD9H00QC921GV1");
        deleteBossAll("20AMSMH02KM31745B");
        deleteBossAll("30U4NMR09775BY57U");
        deleteBossAll("T0BC698092T52BR5Y");
        deleteBossAll("S0MBDMB00VP1L79GM");
        deleteBossAll("T0ANQMN026RSZVCRV");
        deleteBossAll("40MDPM6005G0EN95C");
        deleteBossAll("P0LJ89Y0289GZ2R76");
        deleteBossAll("80ALK9M09PY2E29UF");
        deleteBossAll("N0FBAM402P8K0HA9P");
        deleteBossAll("208889G00GTHA1N4S");
        deleteBossAll("K0T4KMZ00URVZB4BY");
        new JdbcTemplate("457.bdp_equity").update("delete from shareholder_investment_ratio_total_new where company_id = 6135078045");
        new JdbcTemplate("491.prism_shareholder_path").update("delete from ratio_path_company_new_54 where company_id = '761462154'");
        new JdbcTemplate("463.bdp_equity").update("delete from entity_controller_details_new where company_id_controlled = '761462154'");
        new JdbcTemplate("463.bdp_equity").update("delete from entity_controller_details_new where company_id_controlled = '3363932809'");
        new JdbcTemplate("463.bdp_equity").update("delete from entity_controller_details_new where company_id_controlled = '3300025503'");
        new JdbcTemplate("463.bdp_equity").update("delete from entity_controller_details_new where company_id_controlled = '100887933'");
        new JdbcTemplate("463.bdp_equity").update("delete from entity_controller_details_new where company_id_controlled = '2401808361'");
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
