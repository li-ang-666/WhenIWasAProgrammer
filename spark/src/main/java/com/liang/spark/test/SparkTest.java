package com.liang.spark.test;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkTest {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.csv(spark, "tb.csv").createOrReplaceTempView("t");
        spark.sql("select count(distinct id) cnt from t").show();
        spark.sql("select count_distinct(id) cnt from t").show();
    }
}