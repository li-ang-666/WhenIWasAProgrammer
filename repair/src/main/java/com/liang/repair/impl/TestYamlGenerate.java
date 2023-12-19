package com.liang.repair.impl;

import cn.hutool.setting.yaml.YamlUtil;
import com.liang.common.dto.config.DBConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.repair.service.ConfigHolder;

import java.io.PrintWriter;
import java.util.Map;

public class TestYamlGenerate extends ConfigHolder {
    public static void main(String[] args) {
        Map<String, DBConfig> dbConfigs = ConfigUtils.getConfig().getDbConfigs();
        dbConfigs.forEach((name, dbConfig) -> {
            dbConfig.setHost("9349c027b3b4414aa5f9019cd218e7a3in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com");
            dbConfig.setPort(3306);
            dbConfig.setUser("jdtest_d_data_ddl");
            dbConfig.setPassword("dwjIFAmM39Y2O98cKu");
        });
        PrintWriter printWriter = new PrintWriter(System.out);
        System.out.println("dbConfigs:");
        YamlUtil.dump(dbConfigs, printWriter);
    }
}
