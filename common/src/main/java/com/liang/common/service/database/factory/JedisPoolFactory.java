package com.liang.common.service.database.factory;

import com.liang.common.dto.config.RedisConfig;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
public class JedisPoolFactory {
    private JedisPoolFactory() {
    }

    public static JedisPool create(String name) {
        RedisConfig redisConfig = ConfigUtils.getConfig().getRedisConfigs().get(name);
        String host = redisConfig.getHost();
        int port = redisConfig.getPort();
        String password = redisConfig.getPassword();
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPoolConfig.setMinIdle(1);
        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxWaitMillis(5000);
        jedisPoolConfig.setFairness(false);
        jedisPoolConfig.setTestOnBorrow(false);
        jedisPoolConfig.setTestOnReturn(false);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(1000 * 60);
        jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(1000 * 60 * 5);
        jedisPoolConfig.setNumTestsPerEvictionRun(1);

        log.info("jedisPool 加载: {}", redisConfig);
        if (password == null) {
            return new JedisPool(jedisPoolConfig, host, port, 5000);
        }
        return new JedisPool(jedisPoolConfig, host, port, 5000, password);
    }
}
