package com.liang.spark.job;

import com.liang.common.util.ConfigUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class DorisCopyJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(null);
        String source = args[0];
        String sink = "bak." + source.split("\\.")[1];
        // spark
        spark.read().format("doris")
                .option("doris.fenodes", "10.99.197.34:8030,10.99.202.71:8030,10.99.203.88:8030")
                .option("doris.table.identifier", sink)
                .option("user", "dba")
                .option("password", "Tyc@1234")
                .option("doris.batch.size", "10240")
                .option("doris.request.tablet.size", "1")
                .load()
                .repartition()
                .foreachPartition(new DorisJob.DorisSink(ConfigUtils.getConfig(), source.split("\\.")[0], source.split("\\.")[1]));
    }
}
