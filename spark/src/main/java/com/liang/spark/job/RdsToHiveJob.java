package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.spark.sql.SparkSession;

public class RdsToHiveJob {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        String rds = "491.prism_shareholder_path";
        String table = "tyc_group";
        TableFactory
                .jdbc(spark, rds, table)
                .createOrReplaceTempView("t");
        spark.sql("insert overwrite table test.tyc_group select /*+ REPARTITION(32) */ * from t");
    }
}
