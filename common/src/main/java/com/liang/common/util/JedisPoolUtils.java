package com.liang.common.util;

import com.liang.common.database.factory.JedisPoolFactory;
import com.liang.common.dto.config.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JedisPoolUtils {
    private JedisPoolUtils() {
    }

    private static final Map<String, JedisPool> jedisPools = new HashMap<>();

    public static synchronized Jedis getConnection(String name) {
        if (jedisPools.get(name) == null) {
            RedisConfig config = ConfigUtils.getConfig().getRedisConfigs().get(name);
            JedisPool jedisPool = JedisPoolFactory.createConnectionPool(config);
            jedisPools.put(name, jedisPool);
        }
        return jedisPools.get(name).getResource();
    }
}
