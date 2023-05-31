package com.liang.spark.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import com.liang.spark.job.DataConcatJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class SparkSessionFactory {
    private SparkSessionFactory() {
    }

    public static SparkSession createSpark(String[] args) throws Exception {
        initConfig(args);
        return initSpark(args.length > 0);
    }

    private static void initConfig(String[] args) throws Exception {
        InputStream resourceStream;
        if (args.length == 0) {
            log.warn("参数没有传递外部config文件, 从内部 resource 寻找 ...");
            resourceStream = DataConcatJob.class.getClassLoader().getResourceAsStream("config.yml");
        } else {
            log.warn("外部参数传递 config 文件: {}", args[0]);
            resourceStream = Files.newInputStream(Paths.get(args[0]));
        }
        Config config = YamlUtils.parse(resourceStream, Config.class);
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
