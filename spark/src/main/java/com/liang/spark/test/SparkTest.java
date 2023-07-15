package com.liang.spark.test;

import com.liang.spark.basic.TableFactory;
import com.liang.spark.service.SparkHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkTest extends SparkHolder {
    public static void main(String[] args) throws Exception {
        TableFactory.csv(sparkSession, "tb.csv").show(false);
        TableFactory.jdbc(sparkSession, "doris", "stream_load_test").show(false);
    }
}