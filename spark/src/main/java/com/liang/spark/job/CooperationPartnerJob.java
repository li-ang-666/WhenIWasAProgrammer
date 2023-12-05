package com.liang.spark.job;

import com.liang.common.util.ApolloUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class CooperationPartnerJob {
    private final static String[] HUDI_TABLES = new String[]{
            // 主要人员
            "company_human_relation",
            "company_bond_plates",
            "senior_executive",
            "personnel",
            // 股东
            "company_equity_relation_details",
            // 法人
            "company_legal_person",
            // 维表
            "company_index"
    };

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        for (String hudiTable : HUDI_TABLES) {
            spark.read().format("hudi")
                    .load("obs://hadoop-obs/hudi_ods/" + hudiTable)
                    .createOrReplaceGlobalTempView(hudiTable);
        }
        String sqls = ApolloUtils.get("cooperation-partner.sql");
        for (String sql : sqls.split(";")) {
            if (StringUtils.isBlank(sql)) continue;
            log.info("sql: {}", sql);
            spark.sql(sql);
        }
    }
}
