package com.liang.spark.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkSessionFactory {
    private SparkSessionFactory() {
    }

    public static SparkSession createSpark(String[] args) throws Exception {
        initConfig(args);
        return initSpark(args.length > 0);
    }

    private static void initConfig(String[] args) throws Exception {
        Config config = ConfigUtils.initConfig(args);
        ConfigUtils.setConfig(config);
    }

    private static SparkSession initSpark(boolean enableHive) {
        SparkSession.Builder builder = SparkSession
                .builder()
                .config("spark.debug.maxToStringFields", "200");
        if (enableHive) builder.enableHiveSupport();
        return builder.getOrCreate();
    }
}
