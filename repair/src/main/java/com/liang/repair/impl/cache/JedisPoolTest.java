package com.liang.repair.impl.cache;

import com.liang.common.service.database.template.RedisTemplate;
import com.liang.repair.service.ConfigHolder;

public class JedisPoolTest extends ConfigHolder {
    public static void main(String[] args) {
        RedisTemplate redisTemplate = new RedisTemplate("localhost");
        redisTemplate.incr("localhost");
        redisTemplate.incr("abc");
        redisTemplate.incr("abc");
        redisTemplate.incr("abc");
        redisTemplate.incr("abc");
    }

}
