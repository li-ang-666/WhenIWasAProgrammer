package com.liang.spark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHolder {
    private final static SparkSession sparkSession = SparkSession.builder()
            .config("spark.debug.maxToStringFields", "200")
            .master("local[*]")
            .getOrCreate();

    private static Dataset<Row> csv(SparkSession spark, String fileName) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/" + fileName);
    }
}
