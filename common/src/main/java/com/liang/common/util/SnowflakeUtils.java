package com.liang.common.util;

import cn.hutool.core.lang.Snowflake;
import com.liang.common.service.database.template.RedisTemplate;
import lombok.SneakyThrows;
import lombok.Synchronized;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

//@UtilityClass
public class SnowflakeUtils {
    private static volatile Snowflake SNOWFLAKE;

    public static void init(String JobName) {
        if (SNOWFLAKE == null) {
            synchronized (SnowflakeUtils.class) {
                if (SNOWFLAKE == null) {
                    final String LOCK_KEY = JobName + "Lock";
                    final String INCR_KEY = JobName + "Incr";
                    RedisTemplate redisTemplate = new RedisTemplate("metadata");
                    while (!redisTemplate.tryLock(LOCK_KEY)) {
                    }
                    Long incr = redisTemplate.incr(INCR_KEY);
                    redisTemplate.unlock(LOCK_KEY);
                    final long ID = (incr - 1) % 32;
                    SNOWFLAKE = new Snowflake(
                            // 2023-01-01 00:00:00
                            new Date(1672502400L * 1000),
                            ID, ID, false);
                }
            }
        }
    }

    @Test
    public void test() {
        System.out.println(DateTimeUtils.unixTimestamp("2023-01-01 00:00:00"));
    }

    @Synchronized
    @SneakyThrows(InterruptedException.class)
    public static Long nextId() {
        TimeUnit.MILLISECONDS.sleep(1);
        return SNOWFLAKE.nextId();
    }
}
