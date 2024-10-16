package com.liang.common.service.database.holder;

import com.liang.common.service.database.factory.JedisPoolFactory;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JedisPoolHolder implements MultiPoolHolder<JedisPool> {
    private static final Map<String, JedisPool> POOLS = new ConcurrentHashMap<>();

    @Override
    public JedisPool getPool(String name) {
        return POOLS.computeIfAbsent(name, k ->
                new JedisPoolFactory()
                        .createPool(name)
        );
    }

    @Override
    public void closeAll() {
        POOLS.forEach((name, pool) -> {
            try {
                if (!pool.isClosed()) {
                    log.warn("redis close: {}", name);
                    pool.close();
                }
            } catch (Exception ignore) {
            }
        });
    }
}
