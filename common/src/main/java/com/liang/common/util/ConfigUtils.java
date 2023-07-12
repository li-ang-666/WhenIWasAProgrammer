package com.liang.common.util;

import com.liang.common.dto.Config;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.holder.HbaseConnectionHolder;
import com.liang.common.service.database.holder.JedisPoolHolder;
import lombok.Cleanup;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
@UtilityClass
public class ConfigUtils {
    private static volatile Config config;

    public static Config initConfig(String[] args) throws Exception {
        // 加载defaultConfig
        @Cleanup InputStream inputStream = ConfigUtils.class.getClassLoader().getResourceAsStream("default.yml");
        Config defaultConfig = YamlUtils.parse(inputStream, Config.class);
        // 加载customConfig
        if (args.length == 0) {
            log.warn("main(args) 没有传递 config 文件");
            return defaultConfig;
        }
        String fileName = args[0];
        log.info("main(args) 传递 config 文件: {}", fileName);
        Config customConfig = null;
        log.info("try load {} from cluster ...", fileName);
        try {
            @Cleanup InputStream resourceStream1 = Files.newInputStream(Paths.get(fileName));
            customConfig = YamlUtils.parse(resourceStream1, Config.class);
        } catch (Exception e) {
            log.warn("try load {} from package resource ...", fileName);
            try {
                @Cleanup InputStream resourceStream2 = ConfigUtils.class.getClassLoader().getResourceAsStream(fileName);
                customConfig = YamlUtils.parse(resourceStream2, Config.class);
            } catch (Exception ee) {
                log.error("load {} fail", fileName, ee);
            }
        }
        // merge two config
        if (customConfig != null) {
            mergeConfig(defaultConfig, customConfig);
            return customConfig;
        }
        return defaultConfig;
    }

    public static Config getConfig() {
        if (config == null)
            log.error("getConfig(), 返回 null");
        return ConfigUtils.config;
    }

    public static void setConfig(Config config) {
        if (config == null) {
            log.error("setConfig(), 入参为 null");
            return;
        }
        if (ConfigUtils.config == null) {
            synchronized (ConfigUtils.class) {
                if (ConfigUtils.config == null) {
                    ConfigUtils.config = config;
                    log.info("config加载: {}", ConfigUtils.config);
                }
            }
        }
    }

    public static void closeAll() {
        ConfigUtils.config = null;
        new DruidHolder().closeAll();
        new JedisPoolHolder().closeAll();
        new HbaseConnectionHolder().closeAll();
    }

    private static void mergeConfig(Config defaultConfig, Config customConfig) {
        defaultConfig.getDbConfigs().forEach((name, DBConfig) ->
                customConfig.getDbConfigs().putIfAbsent(name, DBConfig)
        );
        defaultConfig.getRedisConfigs().forEach((name, redisConfig) ->
                customConfig.getRedisConfigs().putIfAbsent(name, redisConfig)
        );
        defaultConfig.getHbaseDbConfigs().forEach((name, hbaseDbConfig) ->
                customConfig.getHbaseDbConfigs().putIfAbsent(name, hbaseDbConfig)
        );
        defaultConfig.getDorisDbConfigs().forEach((name, dorisDbConfig) ->
                customConfig.getDorisDbConfigs().putIfAbsent(name, dorisDbConfig)
        );
    }
}
