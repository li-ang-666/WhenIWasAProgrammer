package com.liang.common.database.cluster;

import com.liang.common.database.pool.JedisPoolFactory;
import com.liang.common.dto.config.RedisConfig;
import com.liang.common.util.GlobalUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JedisPoolCluster {
    private JedisPoolCluster() {
    }

    private static final ConcurrentHashMap<String, JedisPool> jedisPools = new ConcurrentHashMap<>();
    private static boolean init = false;

    public static Jedis getConnectionByName(String name) {
        init();
        synchronized (JedisPoolCluster.class) {
            return jedisPools.get(name).getResource();
        }
    }

    private static void init() {
        Map<String, RedisConfig> redisConfigs = GlobalUtils.getConnectionConfig().getRedisConfigs();
        init(redisConfigs);
    }

    private static void init(Map<String, RedisConfig> redisConfigs) {
        if (init) return;
        synchronized (JedisPoolCluster.class) {
            if (init) return;
            redisConfigs.forEach((name, redisConfig) ->
                    jedisPools.put(name, JedisPoolFactory.createConnectionPool(redisConfig))
            );
            init = true;
        }
    }

    public static void release() {
        if (!init) return;
        synchronized (JdbcPoolCluster.class) {
            if (!init) return;
            jedisPools.forEach((k, v) -> v.close());
            init = false;
        }
    }
}
