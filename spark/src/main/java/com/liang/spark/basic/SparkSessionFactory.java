package com.liang.spark.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

@Slf4j
@UtilityClass
public class SparkSessionFactory {
    public static SparkSession createSpark(String[] args) {
        String file = (args != null && args.length > 0) ? args[0] : null;
        initConfig(file);
        return initSpark();
    }

    private static void initConfig(String file) {
        Config config = ConfigUtils.createConfig(file);
        ConfigUtils.setConfig(config);
    }

    private static SparkSession initSpark() {
        SparkSession spark;
        try {
            spark = configSparkBuilder(SparkSession.builder())
                    .enableHiveSupport()
                    .getOrCreate();
        } catch (Exception e) {
            spark = configSparkBuilder(SparkSession.builder())
                    .master("local[*]")
                    .getOrCreate();
        }
        spark.udf().register("count_distinct", functions.udaf(new CountDistinct(), Encoders.STRING()));
        return spark;
    }

    private SparkSession.Builder configSparkBuilder(SparkSession.Builder builder) {
        return builder
                .config("spark.debug.maxToStringFields", "256");
    }
}
