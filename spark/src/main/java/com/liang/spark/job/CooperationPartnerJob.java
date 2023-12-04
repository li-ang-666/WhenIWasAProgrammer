package com.liang.spark.job;

import com.liang.common.util.ApolloUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class CooperationPartnerJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        String sqls = ApolloUtils.get("cooperation-partner.sql");
        for (String sql : sqls.split(";")) {
            if (StringUtils.isBlank(sql)) continue;
            log.info("sql: {}", sql);
            spark.sql(sql);
        }
    }
}
