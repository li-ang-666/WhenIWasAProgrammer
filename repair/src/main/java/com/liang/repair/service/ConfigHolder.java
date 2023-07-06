package com.liang.repair.service;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;


public abstract class ConfigHolder {
    protected final static Logger log = LoggerFactory.getLogger(ConfigHolder.class);

    static {
        InputStream resourceAsStream = ConfigHolder.class.getClassLoader().getResourceAsStream("config.yml");
        Config config = YamlUtils.parse(resourceAsStream, Config.class);
        ConfigUtils.setConfig(config);
    }
}
