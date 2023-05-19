package com.liang.repair.launch;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import com.liang.repair.trait.Runner;

import java.io.InputStream;

public class LocalLauncher {
    public static void main(String[] args) {
        init();
        String className = "LocalTest";
        run(className, args);
        close();
    }

    private static void init() {
        InputStream resourceAsStream = LocalLauncher.class.getClassLoader().getResourceAsStream("config.yml");
        Config config = YamlUtils.parse(resourceAsStream, Config.class);
        ConfigUtils.setConfig(config);
    }

    private static void run(String className, String[] args) {
        try {
            Object instance = Class.forName("com.liang.repair.impl." + className).newInstance();
            ((Runner) instance).run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void close() {
        System.exit(0);
    }
}
