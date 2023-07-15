package com.liang.spark.test;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkTest {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(null);
        TableFactory.csv(spark, "tb.csv").show(false);
        TableFactory.jdbc(spark, "doris", "stream_load_test").show(false);
    }
}