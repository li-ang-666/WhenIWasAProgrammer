package com.liang.common.service.database.factory;

import com.liang.common.dto.config.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
public class JedisPoolFactory {
    private JedisPoolFactory() {
    }

    public static JedisPool create(RedisConfig redisConfig) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        /*---------config---------*/
        jedisPoolConfig.setMinIdle(3);
        jedisPoolConfig.setMaxTotal(5);
        jedisPoolConfig.setMaxWaitMillis(1000 * 10);
        //回收线程1分钟启动一次
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(1000 * 60);
        //回收idle时长5分钟以上的
        jedisPoolConfig.setMinEvictableIdleTimeMillis(1000 * 60 * 5);
        //从池子里拿连接的时候测试一下是不是有效连接
        jedisPoolConfig.setTestOnBorrow(true);
        //回收线程启动的时候测试一下是不是有效连接
        jedisPoolConfig.setTestWhileIdle(true);
        //还给池子的时候测试一下是不是有效连接
        jedisPoolConfig.setTestOnReturn(false);

        log.info("jedis连接池懒加载, url: {}", redisConfig.getHost() + ":" + redisConfig.getPort());
        return new JedisPool(jedisPoolConfig, redisConfig.getHost(), redisConfig.getPort(), 1000 * 60, redisConfig.getPassword());
    }
}
