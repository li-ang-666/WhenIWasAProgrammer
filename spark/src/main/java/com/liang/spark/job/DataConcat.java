package com.liang.spark.job;

import com.liang.spark.dao.DataConcatSqlHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Iterator;

@Slf4j
public class DataConcat {
    public static void main(String[] args) {
        DataConcatSqlHolder sqlHolder = new DataConcatSqlHolder();
        SparkSession spark = getSparkSession();
        Dataset<Row> sql = spark.sql(sqlHolder.RestrictedConsumptionMostApplicantSql());
        sql.show(11111, false);
        log.warn("driver print count: {}", sql.count());
        spark.close();
        sql.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
            }
        });
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

    @Test
    void test() {
        DataConcatSqlHolder sqlHolder = new DataConcatSqlHolder();

    }
}
