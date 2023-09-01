package com.liang.spark.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

@Slf4j
@UtilityClass
public class SparkSessionFactory {
    public static SparkSession createSpark(String[] args) {
        String file = (args != null && args.length > 0) ? args[0] : null;
        initConfig(file);
        // 自定义JdbcDialect
        JdbcDialects.unregisterDialect(JdbcDialects.get("jdbc:mysql"));
        JdbcDialects.registerDialect(new FixedMySQLDialect());
        return initSpark();
    }

    private static void initConfig(String file) {
        Config config = ConfigUtils.createConfig(file);
        ConfigUtils.setConfig(config);
    }

    private static SparkSession initSpark() {
        try {
            return SparkSession
                    .builder()
                    .config("spark.debug.maxToStringFields", "200")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .enableHiveSupport()
                    .getOrCreate();
        } catch (Exception e) {
            return SparkSession.builder()
                    .config("spark.debug.maxToStringFields", "200")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .master("local[*]")
                    .getOrCreate();
        }
    }
}
