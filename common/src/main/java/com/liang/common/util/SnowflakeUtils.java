package com.liang.common.util;

import cn.hutool.core.lang.Snowflake;
import com.liang.common.service.database.template.RedisTemplate;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

@Slf4j
@UtilityClass
public class SnowflakeUtils {
    private static volatile Snowflake SNOWFLAKE;

    public static void init(String JobName) {
        if (SNOWFLAKE == null) {
            synchronized (SnowflakeUtils.class) {
                if (SNOWFLAKE == null) {
                    String lockKey = JobName + "Lock";
                    String incrKey = JobName + "Incr";
                    RedisTemplate redisTemplate = new RedisTemplate("metadata");
                    while (!redisTemplate.tryLock(lockKey)) {
                    }
                    long incr = (redisTemplate.incr(incrKey) - 1) % (32 * 32);
                    long dataCenterId = incr / 32;
                    long workerId = incr % 32;
                    redisTemplate.unlock(lockKey);
                    log.info("Snowflake init, dataCenterId: {}, workerId: {}", dataCenterId, workerId);
                    SNOWFLAKE = new Snowflake(
                            new Date(DateTimeUtils.unixTimestamp("2023-01-01 00:00:00") * 1000L),
                            workerId, dataCenterId, true);
                }
            }
        }
    }

    public static Long nextId() {
        return SNOWFLAKE.nextId();
    }
}
