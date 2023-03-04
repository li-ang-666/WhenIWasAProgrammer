package com.liang.common.database.template;

import redis.clients.jedis.Jedis;

@FunctionalInterface
public interface JedisMapper<T> {
    T map(Jedis jedis) throws Exception;
}
