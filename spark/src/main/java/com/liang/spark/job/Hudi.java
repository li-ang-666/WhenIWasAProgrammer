package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class Hudi {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.read().format("hudi")
                .load("obs://hadoop-obs/hudi/hudi_table")
                .createOrReplaceTempView("hudi_table");
        spark.sql("select count(1) from hudi_table").show();
    }
}
