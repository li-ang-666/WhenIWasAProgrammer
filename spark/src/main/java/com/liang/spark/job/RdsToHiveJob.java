package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.spark.sql.SparkSession;

public class RdsToHiveJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        String rds = "467.company_base";
        String table = "cooperation_partner";
        TableFactory
                .jdbc(spark, rds, table)
                .drop("id", "create_time", "update_time")
                .createOrReplaceTempView("t");
        spark.sql("insert overwrite table hudi_ads.cooperation_partner(pt=1) select /*+ REPARTITION(32) */ * from t");
    }
}
