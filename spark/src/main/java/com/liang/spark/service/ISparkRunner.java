package com.liang.spark.service;

import org.apache.spark.sql.SparkSession;

public interface ISparkRunner {
    void run(SparkSession spark);
}
