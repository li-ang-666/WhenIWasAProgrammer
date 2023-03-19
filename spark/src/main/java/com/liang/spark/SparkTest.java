package com.liang.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

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
        spark.udf().register("countd", new CountDistinct());

        spark.sql("explain select channel_id,count(distinct candidate_id),collect_list(distinct application_id),sum(candidate_id) from t group by channel_id order by channel_id")
                .show(false);


        spark.sql("explain select t1.* from t t1 join t t2 on t1.channel_id=t2.channel_id")
                .show(false);

        spark.sql("explain select channel_id,countd(candidate_id),countd(application_id),sum(candidate_id) from t group by channel_id")
                .show(false);

       // Thread.sleep(1000*3600);
        spark.stop();
    }
}