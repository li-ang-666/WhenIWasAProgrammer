package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class DimCountJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);

    }
}
