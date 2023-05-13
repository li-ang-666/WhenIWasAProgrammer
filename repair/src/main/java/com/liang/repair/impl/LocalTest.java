package com.liang.repair.impl;

import com.liang.common.service.database.template.RedisEnhancer;
import com.liang.repair.trait.Runner;

import java.util.Set;

public class LocalTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        RedisEnhancer localhost = new RedisEnhancer("localhost");
        Set<String> res = localhost.exec(j -> j.keys("*"));
        for (String re : res) {
            System.out.println(re + " -> " + localhost.exec(j -> j.get(re)));
        }

    }
}
