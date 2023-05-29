package com.liang.spark.job;

import com.liang.spark.dao.DataConcatSqlHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class DataConcat {
    public static void main(String[] args) {
        DataConcatSqlHolder sqlHolder = new DataConcatSqlHolder();
        SparkSession spark = getSparkSession();
        Dataset<Row> sql = spark.sql(sqlHolder.queryMostApplicantSql());
        sql.show(11111, false);
        log.warn("driver print count: {}", sql.count());
        spark.close();
    }

    public static SparkSession getSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.debug.maxToStringFields", "200")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("use ods");
        return spark;
    }
}
