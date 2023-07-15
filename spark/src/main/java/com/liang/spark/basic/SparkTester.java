package com.liang.spark.basic;

import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@UtilityClass
public class SparkTester {
    public static void test(SparkConsumer consumer) {
        SparkSession spark = SparkSession.builder()
                .config("spark.debug.maxToStringFields", "200")
                .master("local[*]")
                .getOrCreate();
        consumer.consume(spark);
    }

    public static Dataset<Row> csv(SparkSession spark, String fileName) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/" + fileName);
    }

    @FunctionalInterface
    public interface SparkConsumer {
        void consume(SparkSession sparkSession);
    }
}
