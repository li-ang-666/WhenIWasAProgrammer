package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.service.LocalConfigFile;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@LocalConfigFile("java-jar.yml")
public class JavaJarJob {
    public static void main(String[] args) throws Exception {
        EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
    }
}
