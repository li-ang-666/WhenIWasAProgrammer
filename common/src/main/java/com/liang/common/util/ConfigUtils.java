package com.liang.common.util;

import com.liang.common.dto.Config;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.holder.HbaseConnectionHolder;
import com.liang.common.service.database.holder.JedisPoolHolder;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

@Slf4j
public class ConfigUtils {
    private static volatile Config config;

    private ConfigUtils() {
    }

    public static Config initConfig(String[] args) throws Exception {
        InputStream resourceStream;
        if (args.length == 0) {
            throw new RuntimeException("main(args) 没有传递 config 文件, program exit ...");
        }
        String fileName = args[0];
        log.info("main(args) 传递 config 文件: {}", fileName);
        try {
            log.info("try load {} from cluster ...", fileName);
            resourceStream = Files.newInputStream(Paths.get(fileName));
        } catch (NoSuchFileException e) {
            log.warn("try load {} from package resource ...", fileName);
            resourceStream = ConfigUtils.class.getClassLoader().getResourceAsStream(fileName);
        }
        return YamlUtils.parse(resourceStream, Config.class);
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
}
