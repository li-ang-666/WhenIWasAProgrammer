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
                .option("doris.fenodes", "10.99.197.34:8030,10.99.202.71:8030,10.99.203.88:8030")
                .option("doris.table.identifier", args[0] + "." + args[1])
                .option("user", "dba")
                .option("password", "Tyc@1234")
                .option("doris.batch.size", "1024000")
                .load()
                .foreachPartition(new DorisJob.DorisSink(ConfigUtils.getConfig(), args[2], args[3]));
    }
}
