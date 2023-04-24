package com.liang.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.nio.ByteBuffer;

public class SparkTest {
    @Test
    public void test() throws Throwable {
        ByteBuffer allocate = ByteBuffer.allocate(11);
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
        spark.udf().register("countd", new CountDistinct());

        spark.sql("explain select col_dim,count(distinct col_a),count(distinct col_b),max(col_c),min(col_d) from t group by col_dim")
                .show(false);

        spark.stop();
    }
}