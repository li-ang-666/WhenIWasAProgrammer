package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class QueryHudiJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.read().format("hudi")
                .load("obs://hadoop-obs/hudi/hudi_table")
                .createOrReplaceTempView("hudi_table");
        spark.sql("select count(distinct id) from hudi_table").show();
    }
}
