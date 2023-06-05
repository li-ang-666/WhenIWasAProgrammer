package com.liang.spark.service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSparkRunner implements ISparkRunner {
    protected String preQuery(String sql) {
        log.info("执行SQL: {}", sql);
        return sql;
    }
}
