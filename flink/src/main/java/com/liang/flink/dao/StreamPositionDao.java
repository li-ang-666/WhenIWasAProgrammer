package com.liang.flink.dao;

import com.liang.common.database.template.JedisTemplate;
import com.liang.common.service.Lists;
import com.liang.flink.dto.StreamSinkInfo;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;
import java.util.UUID;

/**
 * 0 -> null,
 * 1 -> 甲###1630486802570,
 * 2 -> 乙###1630486802575,
 * 3 -> null,
 * 4 -> null,
 * 5 -> 丙###1630486802580
 */
public class StreamPositionDao {
    private static final String LOCK_KEY = "liang_lock";
    private static final String SINK_KEY = "liang_sink";
    public static final String NO_USED = "NO_USED";

    public static final int HEARTBEAT_TIMEOUT = 1000 * 10;
    public static final int LOCK_TTL = 5;

    private JedisTemplate jedisTemplate = new JedisTemplate("redistest");

    /**
     * NX 之前不存在则返回OK
     * XX 之前已存在则返回OK
     * EX 秒
     * PX 毫秒
     */
    public String tryLock() {
        return jedisTemplate.exec(jedis -> jedis.set(LOCK_KEY, String.valueOf(UUID.randomUUID()), "NX", "EX", LOCK_TTL));
    }

    public void unlock() {
        jedisTemplate.exec(jedis -> jedis.del(LOCK_KEY));
    }

    public Map<Integer, StreamSinkInfo> getSinkMap() {
        return Lists.of(jedisTemplate.hScan(SINK_KEY))
                .map(kv -> Tuple2.of(
                        Integer.parseInt(kv.f0),
                        kv.f1.equals(NO_USED) ? null : new StreamSinkInfo(kv.f1)))
                .toMap(e -> e.f0, e -> e.f1);
    }

    public void returnPartition(Object partition) {
        jedisTemplate.exec(jedis -> jedis.hset(SINK_KEY, String.valueOf(partition), NO_USED));
    }

    public void registerPartition(Object partition, String orgId) {
        jedisTemplate.exec(jedis -> jedis.hset(SINK_KEY, String.valueOf(partition), orgId + "###" + -1));
    }

    public void flushPartition(Object partition, String orgId) {
        jedisTemplate.exec(jedis -> jedis.hset(SINK_KEY, String.valueOf(partition), orgId + "###" + System.currentTimeMillis()));
    }
}
