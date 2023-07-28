package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class RatioPathCompanyDeleteJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "prismShareholderPath", "investment_relation")
                .createOrReplaceTempView("investment_relation");

        TableFactory.jdbc(spark, "prismShareholderPath", "ratio_path_company")
                .createOrReplaceTempView("ratio_path_company");


        spark.sql("select distinct t1.company_id from ratio_path_company t1 left join investment_relation t2 on t1.company_id = t2.company_id_invested where t2.company_id_invested is null")
                .createOrReplaceTempView("t");

        spark.sql("select * from t limit 1000")
                .show(1000, false);
        log.info("size: {}", spark.sql("select * from t").collectAsList().size());
    }
}
