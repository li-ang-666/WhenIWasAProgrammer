package com.liang.spark.job;

import com.liang.common.util.ApolloUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL;

@Slf4j
public class CooperationPartnerJob {
    private final static List<String> HUDI_TABLES = new ArrayList<>();

    static {
        // 主要人员
        HUDI_TABLES.add("company_human_relation");
        HUDI_TABLES.add("company_bond_plates");
        HUDI_TABLES.add("senior_executive");
        HUDI_TABLES.add("personnel");
        // 股东
        HUDI_TABLES.add("company_equity_relation_details");
        // 法人
        HUDI_TABLES.add("company_legal_person");
        // 维表
        HUDI_TABLES.add("company_index");
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        for (String hudiTable : HUDI_TABLES) {
            String path = String.format("obs://hadoop-obs/hudi_ods/%s/", hudiTable);
            log.info("load hudi: {} -> {}", hudiTable, path);
            spark.read().format("hudi")
                    .option(QUERY_TYPE().key(), QUERY_TYPE_READ_OPTIMIZED_OPT_VAL())
                    .load(path)
                    .createOrReplaceTempView(hudiTable);
        }
        String sqls = ApolloUtils.get("cooperation-partner.sql");
        for (String sql : sqls.split(";")) {
            if (StringUtils.isBlank(sql)) continue;
            log.info("sql: {}", sql);
            spark.sql(sql);
        }
    }
}
