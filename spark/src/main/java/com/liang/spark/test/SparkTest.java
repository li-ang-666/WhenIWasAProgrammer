package com.liang.spark.test;

import com.liang.spark.basic.SparkTester;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SparkTest {
    public static void main(String[] args) throws Exception {
        SparkTester.test(spark -> {
            SparkTester.getCSVTable(spark, "tb.csv")
                    .show(false);
        });
    }
}