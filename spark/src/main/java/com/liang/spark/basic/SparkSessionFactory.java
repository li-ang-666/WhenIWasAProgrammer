package com.liang.spark.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
@UtilityClass
public class SparkSessionFactory {
    public static SparkSession createSpark(String[] args) {
        String file = (args != null && args.length > 0) ? args[0] : null;
        initConfig(file);
        return initSpark();
    }

    private static void initConfig(String file) {
        Config config = ConfigUtils.initConfig(file);
        ConfigUtils.setConfig(config);
    }

    private static SparkSession initSpark() {
        try {
            return SparkSession
                    .builder()
                    .config("spark.debug.maxToStringFields", "200")
                    .enableHiveSupport()
                    .getOrCreate();
        } catch (Exception e) {
            return SparkSession.builder()
                    .config("spark.debug.maxToStringFields", "200")
                    .master("local[*]")
                    .getOrCreate();
        }
    }
}
