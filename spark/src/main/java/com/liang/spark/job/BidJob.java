package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class BidJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"mysql-to-hive.yml"};
        }
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.sql("use test");

    }

    private static void get(SparkSession spark, String tableName) {
        spark.sql(String.format("select '%s',mid,* from %s",tableName,tableName));
    }
}
