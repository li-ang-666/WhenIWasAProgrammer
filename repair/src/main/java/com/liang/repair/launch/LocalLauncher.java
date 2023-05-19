package com.liang.repair.launch;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import com.liang.repair.annotation.Prop;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;

@Slf4j
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
            Class<?> aClass = Class.forName("com.liang.repair.impl." + className);

            Prop prop = aClass.getAnnotation(Prop.class);
            System.out.println(prop);

            ((Runner) aClass.newInstance()).run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void close() {
        System.exit(0);
    }
}
