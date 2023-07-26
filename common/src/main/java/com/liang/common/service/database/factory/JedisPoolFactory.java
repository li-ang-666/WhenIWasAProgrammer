package com.liang.common.service.database.factory;

import com.liang.common.dto.config.RedisConfig;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
public class JedisPoolFactory implements IFactory<JedisPool> {

    @Override
    public JedisPool createPool(String name) {
        try {
            RedisConfig redisConfig = ConfigUtils.getConfig().getRedisConfigs().get(name);
            String host = redisConfig.getHost();
            int port = redisConfig.getPort();
            String password = redisConfig.getPassword();
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

            jedisPoolConfig.setMinIdle(1);
            jedisPoolConfig.setMaxIdle(10);
            jedisPoolConfig.setMaxTotal(10);
            jedisPoolConfig.setMaxWaitMillis(5000);
            jedisPoolConfig.setTestOnBorrow(false);
            jedisPoolConfig.setTestOnReturn(false);
            jedisPoolConfig.setTestWhileIdle(true);
            jedisPoolConfig.setTimeBetweenEvictionRunsMillis(1000 * 60);
            jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(1000 * 60 * 5);
            jedisPoolConfig.setNumTestsPerEvictionRun(1);

            log.info("jedisPool 加载: {}", redisConfig);
            return new JedisPool(jedisPoolConfig, host, port, 1000 * 30, password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
