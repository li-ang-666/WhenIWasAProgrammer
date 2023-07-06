package com.liang.spark.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
@UtilityClass
public class SparkSessionFactory {
    public static SparkSession createSpark(String[] args) throws Exception {
        initConfig(args);
        return initSpark();
    }

    private static void initConfig(String[] args) throws Exception {
        Config config = ConfigUtils.initConfig(args);
        ConfigUtils.setConfig(config);
    }

    private static SparkSession initSpark() {
        return SparkSession
                .builder()
                .config("spark.debug.maxToStringFields", "200")
                .enableHiveSupport().getOrCreate();
    }
}
