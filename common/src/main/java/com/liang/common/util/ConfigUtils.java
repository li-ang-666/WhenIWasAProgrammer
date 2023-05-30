package com.liang.common.util;

import com.liang.common.dto.Config;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigUtils {
    private static volatile Config config;

    private ConfigUtils() {
    }

    public static Config getConfig() {
        if (config == null) {
            log.error("ConfigUtils 未初始化");
        }
        return config;
    }

    public static void setConfig(Config config) {
        if (ConfigUtils.config == null) {
            synchronized (ConfigUtils.class) {
                if (ConfigUtils.config == null) {
                    ConfigUtils.config = config;
                    log.info("config加载: {}", config);
                }
            }
        }
    }
}
