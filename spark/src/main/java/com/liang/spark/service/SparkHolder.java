package com.liang.spark.service;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import org.apache.spark.sql.SparkSession;

public abstract class SparkHolder {
    protected final static SparkSession sparkSession = SparkSession.builder()
            .config("spark.debug.maxToStringFields", "200")
            .master("local[*]")
            .getOrCreate();

    static {
        Config config = ConfigUtils.initConfig(null);
        ConfigUtils.setConfig(config);
    }
}
