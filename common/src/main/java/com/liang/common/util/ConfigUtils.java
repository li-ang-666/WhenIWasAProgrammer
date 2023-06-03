package com.liang.common.util;

import com.liang.common.dto.Config;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.holder.HbaseConnectionHolder;
import com.liang.common.service.database.holder.JedisPoolHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigUtils {
    private static volatile Config config;

    private ConfigUtils() {
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
