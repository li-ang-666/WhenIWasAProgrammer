package com.liang.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Objects;

public class SparkTest {
    @Test
    public void test() throws Throwable {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .config("sspark.sql.shuffle.partitions", "20")
                .master("local[*]")
                .getOrCreate();
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/t3.csv")
                .createTempView("t");

        spark.sql("select datediff(date,'2000-01-01'),name,date,cost,sum(cost)over(partition by date order by date_sub(date,'2000-01-01') range between unbounded preceding and unbounded following) n " +
                "from t").show();
        spark.stop();
    }
}
