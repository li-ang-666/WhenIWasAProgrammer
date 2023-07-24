package com.liang.common.util;

import cn.hutool.core.lang.Snowflake;
import com.liang.common.service.database.template.RedisTemplate;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SnowflakeUtils {
    private static volatile Snowflake SNOWFLAKE;

    public static void init(String JobName) {
        final String LOCK_KEY = JobName + "Lock";
        final String INCR_KEY = JobName + "Incr";
        if (SNOWFLAKE == null) {
            synchronized (SnowflakeUtils.class) {
                if (SNOWFLAKE == null) {
                    RedisTemplate redisTemplate = new RedisTemplate("metadata");
                    while (!redisTemplate.tryLock(LOCK_KEY)) {
                    }
                    Long incr = redisTemplate.incr(INCR_KEY);
                    redisTemplate.unlock(LOCK_KEY);
                    SNOWFLAKE = new Snowflake((incr - 1) % 32);
                }
            }
        }
    }

    public static Long nextId() {
        return SNOWFLAKE.nextId();
    }
}
