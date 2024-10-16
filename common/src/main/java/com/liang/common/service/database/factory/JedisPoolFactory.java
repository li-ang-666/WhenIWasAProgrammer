package com.liang.common.service.database.factory;

import com.liang.common.dto.config.RedisConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JedisPoolFactory implements PoolFactory<RedisConfig, JedisPool> {
    private final static JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

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
            JedisPool jedisPool = new JedisPool(jedisPoolConfig, host, port, (int) TimeUnit.MINUTES.toMillis(5), password);
            log.info("JedisPoolFactory createPool success, config: {}", JsonUtils.toString(config));
            return jedisPool;
        } catch (Exception e) {
            String msg = "JedisPoolFactory createPool error, config: " + JsonUtils.toString(config);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }
}
