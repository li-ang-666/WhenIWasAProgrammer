package com.liang.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.collection.Seq;

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
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/t2.csv")
                .createTempView("t2");

        spark.sql("explain select * from t1 left join t2 where t1.id > t2.id").show(false);

        spark.stop();
    }
}
