package com.liang.repair.service;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;

import java.io.InputStream;

public class ConfigHolder {
    static {
        InputStream resourceAsStream = ConfigHolder.class.getClassLoader().getResourceAsStream("config.yml");
        Config config = YamlUtils.parse(resourceAsStream, Config.class);
        ConfigUtils.setConfig(config);
    }
}
