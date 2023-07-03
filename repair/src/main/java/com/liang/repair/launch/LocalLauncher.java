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
    public static void main(String[] args) throws Exception {
        //初始化
        init();

        //类加载
        String className = "Test2";
        Class<?> aClass = Class.forName("com.liang.repair.impl." + className);

        //测试注解
        if (aClass.isAnnotationPresent(Prop.class))
            System.out.println(aClass.getAnnotation(Prop.class));

        //程序执行
        ((Runner) aClass.newInstance()).run(args);
    }

    private static void init() {
        InputStream resourceAsStream = LocalLauncher.class.getClassLoader().getResourceAsStream("config.yml");
        Config config = YamlUtils.parse(resourceAsStream, Config.class);
        ConfigUtils.setConfig(config);
    }
}
