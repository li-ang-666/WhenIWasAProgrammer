package com.liang.spark.service;

import org.apache.spark.sql.SparkSession;

public abstract class SparkHolder {
    private final static SparkSession sparkSession = SparkSession.builder()
            .config("spark.debug.maxToStringFields", "200")
            .master("local[*]")
            .getOrCreate();
}
