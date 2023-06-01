package com.liang.spark.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public abstract class AbstractSparkRunner {
    public abstract void run(SparkSession spark);

    protected String preQuery(String sql) {
        log.info("执行SQL: {}", sql);
        return sql;
    }
}
