package com.liang.common.service.database.holder;

import com.liang.common.service.database.factory.JedisPoolFactory;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JedisPoolHolder {
    private static final Map<String, JedisPool> jedisPools = new ConcurrentHashMap<>();

    private JedisPoolHolder() {
    }

    public static synchronized JedisPool getJedisPool(String name) {
        if (jedisPools.get(name) == null) {
            JedisPool jedisPool = JedisPoolFactory.create(name);
            JedisPool callback = jedisPools.putIfAbsent(name, jedisPool);
            //说明这次put已经有值了
            if (callback != null) {
                log.warn("putIfAbsent() fail, delete redundant jedisPool: {}", name);
                try {
                    jedisPool.close();
                } catch (Exception ignore) {
                }
                jedisPool = null;//help gc
            }
        }
        return jedisPools.get(name);
    }
}
