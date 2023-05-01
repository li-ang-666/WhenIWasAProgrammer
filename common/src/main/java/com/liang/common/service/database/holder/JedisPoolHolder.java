package com.liang.common.service.database.holder;

import com.liang.common.dto.config.RedisConfig;
import com.liang.common.service.database.factory.JedisPoolFactory;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JedisPoolHolder {
    private JedisPoolHolder() {
    }

    private static final Map<String, JedisPool> jedisPools = new HashMap<>();

    public static synchronized Jedis getConnection(String name) {
        if (jedisPools.get(name) == null) {
            synchronized (JedisPoolHolder.class) {
                if (jedisPools.get(name) == null) {
                    RedisConfig config = ConfigUtils.getConfig().getRedisConfigs().get(name);
                    JedisPool jedisPool = JedisPoolFactory.create(config);
                    jedisPools.put(name, jedisPool);
                }
            }
        }
        return jedisPools.get(name).getResource();
    }
}
