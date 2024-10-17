package com.liang.common.service.database.factory;

import com.liang.common.dto.config.RedisConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

@Slf4j
public class JedisPoolFactory implements SinglePoolFactory<RedisConfig, JedisPool> {
    private final static JedisPoolConfig JEDIS_POOL_CONFIG = new JedisPoolConfig();

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(1);
        jedisPoolConfig.setMaxIdle(128);
        jedisPoolConfig.setMaxTotal(128);
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
        return createPool(ConfigUtils.getConfig().getRedisConfigs().get(name));
    }

    @Override
    public JedisPool createPool(RedisConfig config) {
        try {
            String host = config.getHost();
            int port = config.getPort();
            String password = config.getPassword();
            JedisPool jedisPool = new JedisPool(JEDIS_POOL_CONFIG, host, port, (int) MILLISECONDS.convert(5, MINUTES), password);
            log.info("JedisPoolFactory createPool success, config: {}", JsonUtils.toString(config));
            return jedisPool;
        } catch (Exception e) {
            String msg = "JedisPoolFactory createPool error, config: " + JsonUtils.toString(config);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }
}
