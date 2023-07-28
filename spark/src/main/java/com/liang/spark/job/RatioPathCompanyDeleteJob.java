package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class RatioPathCompanyDeleteJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "prismShareholderPath", "investment_relation")
                .createOrReplaceTempView("t1");
        TableFactory.jdbc(spark, "prismShareholderPath", "ratio_path_company")
                .createOrReplaceTempView("t2");
        spark.sql("insert overwrite table test.company_id_test select /*+ REPARTITION(1200) */ * from (select company_id_invested from t1 union all select company_id from t2) t");
    }
}
