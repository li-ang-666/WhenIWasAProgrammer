package com.liang.common.service.database.holder;

import com.liang.common.dto.config.RedisConfig;
import com.liang.common.service.database.factory.JedisPoolFactory;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JedisPoolHolder {
    private JedisPoolHolder() {
    }

    private static final Map<String, JedisPool> jedisPools = new ConcurrentHashMap<>();

    public static synchronized JedisPool getJedisPool(String name) {
        if (jedisPools.get(name) == null) {
            RedisConfig config = ConfigUtils.getConfig().getRedisConfigs().get(name);
            JedisPool jedisPool = JedisPoolFactory.create(config);
            JedisPool callback = jedisPools.putIfAbsent(name, jedisPool);
            //说明这次put已经有值了
            if (callback != null) {
                jedisPool = null;//help gc
            }
        }
        return jedisPools.get(name);
    }
}
