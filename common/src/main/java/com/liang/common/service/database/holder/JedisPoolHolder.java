package com.liang.common.service.database.holder;

import com.liang.common.service.database.factory.JedisPoolFactory;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JedisPoolHolder implements IHolder<JedisPool> {
    private static final Map<String, JedisPool> pools = new ConcurrentHashMap<>();

    @Override
    public JedisPool getPool(String name) {
        if (pools.get(name) == null) {
            JedisPool jedisPool = new JedisPoolFactory().createPool(name);
            JedisPool callback = pools.putIfAbsent(name, jedisPool);
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
        return pools.get(name);
    }

    @Override
    public void closeAll() {
        for (Map.Entry<String, JedisPool> entry : pools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            if (!jedisPool.isClosed()) {
                log.warn("jedisPool close: {}", entry.getKey());
                jedisPool.close();
            }
        }
    }
}
