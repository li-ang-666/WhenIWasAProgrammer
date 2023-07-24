package com.liang.repair.impl;

import cn.hutool.core.lang.Snowflake;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.repair.service.ConfigHolder;

public class SnowIdTest extends ConfigHolder {
    private static volatile Snowflake SNOWFLAKE;

    public static void main(String[] args) {
        RedisTemplate redisTemplate = new RedisTemplate("metadata");
        while (!redisTemplate.tryLock("aaa")) {
        }
        log.info("get lock");
        if (SNOWFLAKE == null) {
            synchronized (SnowIdTest.class) {
                if (SNOWFLAKE == null) {
                    Long incr = redisTemplate.incr("bbb");
                    SNOWFLAKE = new Snowflake(incr);
                }
            }
        }
        redisTemplate.unlock("aaa");
    }
}
