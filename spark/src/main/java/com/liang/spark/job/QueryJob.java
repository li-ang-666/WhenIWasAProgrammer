package com.liang.spark.job;

import com.liang.common.util.ApolloUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

@Slf4j
public class QueryJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.read()
                .format("hudi")
                //.option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL())
                .load("obs://hadoop-obs/hudi_ods/ratio_path_company005")
                .createOrReplaceTempView("hudi_table");
        String sql = ApolloUtils.get("spark");
        log.info("sql: {}", sql);
        List<Row> rows = spark.sql(sql).collectAsList();
        for (Row row : rows) {
            log.info("{}", row.json());
        }
        log.info("done");
    }
}
