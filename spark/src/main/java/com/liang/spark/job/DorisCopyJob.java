package com.liang.spark.job;

import com.liang.common.util.ConfigUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class DorisCopyJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(null);
        spark.read().format("doris")
                .option("doris.fenodes", "")
                .option("doris.table.identifier", "bak.test_ads_user_tag")
                .option("user", "admin")
                .option("password", "Tyc@1234")
                .option("doris.batch.size", "10240")
                .option("doris.request.tablet.size", "1")
                .load()
                .repartition(1)
                .foreachPartition(new DorisJob.DorisSink(ConfigUtils.getConfig(), "ads", "ads_user_tag_v2"));
    }
}
