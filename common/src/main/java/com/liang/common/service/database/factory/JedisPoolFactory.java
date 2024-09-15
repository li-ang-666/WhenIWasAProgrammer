package com.liang.common.service.database.factory;

import com.liang.common.dto.config.RedisConfig;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JedisPoolFactory implements IFactory<JedisPool> {
    private final static JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(1);
        jedisPoolConfig.setMaxIdle(16);
        jedisPoolConfig.setMaxTotal(16);
        jedisPoolConfig.setMaxWait(Duration.ofMinutes(5));
        jedisPoolConfig.setTestOnBorrow(false);
        jedisPoolConfig.setTestOnReturn(false);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
        jedisPoolConfig.setMinEvictableIdleTime(Duration.ofSeconds(60));
        jedisPoolConfig.setSoftMinEvictableIdleTime(Duration.ofSeconds(60));
        jedisPoolConfig.setNumTestsPerEvictionRun(1);
    }

    @Override
    public JedisPool createPool(String name) {
        try {
            RedisConfig redisConfig = ConfigUtils.getConfig().getRedisConfigs().get(name);
            String host = redisConfig.getHost();
            int port = redisConfig.getPort();
            String password = redisConfig.getPassword();
            log.info("jedisPool 加载: {}", redisConfig);
            return new JedisPool(jedisPoolConfig, host, port, (int) TimeUnit.MINUTES.toMillis(5), password);
        } catch (Exception e) {
            log.error("JedisPoolFactory createPool error, name: {}", name, e);
            throw new RuntimeException(e);
        }
    }
}
