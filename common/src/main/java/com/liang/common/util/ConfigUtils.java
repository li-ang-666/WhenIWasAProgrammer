package com.liang.common.util;

import com.liang.common.dto.Config;

public class ConfigUtils {
    private static volatile Config config;

    private ConfigUtils() {
    }

    public static void setConfig(Config config) {
        if (ConfigUtils.config == null) {
            synchronized (ConfigUtils.class) {
                if (ConfigUtils.config == null) {
                    ConfigUtils.config = config;
                }
            }
        }
    }

    public static Config getConfig() {
        return ConfigUtils.config;
    }
}
