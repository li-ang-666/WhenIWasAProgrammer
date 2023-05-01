package com.liang.common.service.database.template;

import com.liang.common.service.Timer;
import com.liang.common.service.database.holder.JedisPoolHolder;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RedisEnhancer {
    private final String name;

    public RedisEnhancer(String name) {
        this.name = name;
    }

    public <T> T exec(JedisMapper<T> jedisMapper) {
        T t;
        Timer timer = new Timer();
        try (Jedis jedis = JedisPoolHolder.getConnection(name)) {
            t = jedisMapper.map(jedis);
        } catch (Exception e) {
            log.error("RedisEnhancer Error", e);
            t = null;
        }
        log.debug(timer.getTimeMs() + " ms");
        return t;
    }

    /**
     * 127.0.0.1:6379> HSCAN ${key} ${cursor} [MATCH pattern] [COUNT count]
     * HSCAN 命令每次被调用之后, 都会向用户返回一个新的游标, 用户在下次迭代时需要使用这个新游标作为 HSCAN 命令的游标参数, 以此来延续之前的迭代
     * 当 SCAN 命令的游标参数被设置为 0 时, 服务器将开始一次新的迭代, 而当服务器向用户返回值为 0 的游标时, 表示迭代已结束
     */
    public Map<String, String> hScan(String key) {
        Map<String, String> result = new HashMap<>();
        Timer timer = new Timer();
        try (Jedis jedis = JedisPoolHolder.getConnection(name)) {
            String cursor = ScanParams.SCAN_POINTER_START; //其实就是 "0"
            ScanParams scanParams = new ScanParams().match("*").count(100);
            do {
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(key, cursor, scanParams);
                cursor = scanResult.getStringCursor();
                scanResult.getResult().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
            } while (!"0".equals(cursor));
        } catch (Exception e) {
            log.error("RedisEnhancer Error", e);
        }
        log.debug(timer.getTimeMs() + " ms");
        return result;
    }

    public List<String> scan(String key) {
        List<String> result = new ArrayList<>();
        Timer timer = new Timer();
        try (Jedis jedis = JedisPoolHolder.getConnection(name)) {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match(key).count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getStringCursor();
                result.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));
        } catch (Exception e) {
            log.error("RedisEnhancer Error", e);
        }
        log.debug(timer.getTimeMs() + " ms");
        return result;
    }

    public boolean tryLock(String key, int milliseconds) {
        try (Jedis jedis = JedisPoolHolder.getConnection(name)) {
            String res = jedis.set(key, "lock", "NX", "PX", milliseconds);
            return "OK".equals(res);
        } catch (Exception e) {
            log.error("RedisEnhancer Error", e);
            return false;
        }
    }

    public void unlock(String key) {
        exec(jedis -> jedis.del(key));
    }
}
