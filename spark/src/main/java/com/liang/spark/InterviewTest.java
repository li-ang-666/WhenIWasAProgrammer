package com.liang.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class InterviewTest {
    @Test
    public void test() throws Throwable {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .master("local[*]")
                .getOrCreate();
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/t1.csv")
                .createTempView("t1");

        spark.sql("select * from t1").show();

        spark.stop();
    }
}
