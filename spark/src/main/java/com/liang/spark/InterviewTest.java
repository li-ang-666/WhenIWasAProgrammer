package com.liang.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class InterviewTest {
    @Test
    public void test() {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .master("local[*]")
                .getOrCreate();
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/tb1.csv")
                .createTempView("t1");
    }
}
