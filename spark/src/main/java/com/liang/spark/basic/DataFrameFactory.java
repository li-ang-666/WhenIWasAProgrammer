package com.liang.spark.basic;

import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@UtilityClass
public class DataFrameFactory {
    public static Dataset<Row> create(SparkSession spark, String fileName) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/" + fileName);
    }
}
