package com.liang.common.service.database.template;

import redis.clients.jedis.Jedis;

@FunctionalInterface
public interface JedisMapper<T> {
    T map(Jedis jedis) throws Exception;
}
