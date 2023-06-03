package com.liang.common.service.database.template;

import com.liang.common.service.database.holder.JedisPoolHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RedisTemplate {
    private final JedisPool pool;
    private final TemplateLogger logger;

    public RedisTemplate(String name) {
        pool = new JedisPoolHolder().getPool(name);
        logger = new TemplateLogger(this.getClass().getSimpleName(), name);
    }

    public String get(String key) {
        logger.beforeExecute("get", key);
        String res = null;
        try (Jedis jedis = pool.getResource()) {
            res = jedis.get(key);
        } catch (Exception e) {
            logger.ifError("get", key, e);
        }
        logger.afterExecute("get", key);
        return res;
    }

    public void set(String key, String value) {
        logger.beforeExecute("set", key + " -> " + value);
        try (Jedis jedis = pool.getResource()) {
            jedis.set(key, value);
        } catch (Exception e) {
            logger.ifError("set", key + " -> " + value, e);
        }
        logger.afterExecute("set", key + " -> " + value);
    }

    public void del(String key) {
        logger.beforeExecute("del", key);
        try (Jedis jedis = pool.getResource()) {
            jedis.del(key);
        } catch (Exception e) {
            logger.ifError("del", key, e);
        }
        logger.afterExecute("del", key);
    }

    /**
     * <p>127.0.0.1:6379> HSCAN ${key} ${cursor} [MATCH pattern] [COUNT count]
     * <p>HSCAN 命令每次被调用之后, 都会向用户返回一个新的游标, 用户在下次迭代时需要使用这个新游标作为 HSCAN 命令的游标参数, 以此来延续之前的迭代
     * <p>当 SCAN 命令的游标参数被设置为 0 时, 服务器将开始一次新的迭代, 而当服务器向用户返回值为 0 的游标时, 表示迭代已结束
     */

    public Map<String, String> hScan(String key) {
        logger.beforeExecute("hScan", key);
        Map<String, String> result = new HashMap<>();
        try (Jedis jedis = pool.getResource()) {
            String cursor = ScanParams.SCAN_POINTER_START; //其实就是 "0"
            ScanParams scanParams = new ScanParams().match("*").count(100);
            do {
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(key, cursor, scanParams);
                cursor = scanResult.getStringCursor();
                scanResult.getResult().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
            } while (!"0".equals(cursor));
        } catch (Exception e) {
            logger.ifError("hScan", key, e);
        }
        logger.afterExecute("hScan", key);
        return result;
    }

    public List<String> scan() {
        logger.beforeExecute("scan", "");
        List<String> result = new ArrayList<>();
        try (Jedis jedis = pool.getResource()) {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match("*").count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getStringCursor();
                result.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));
        } catch (Exception e) {
            logger.ifError("scan", "", e);
        }
        logger.afterExecute("scan", "");
        return result;
    }

    public boolean tryLock(String key, int milliseconds) {
        logger.beforeExecute("tryLock", Tuple2.of(key, milliseconds));
        boolean res = false;
        try (Jedis jedis = pool.getResource()) {
            String back = jedis.set(key, "lock", "NX", "PX", milliseconds);
            res = ("OK".equals(back));
        } catch (Exception e) {
            logger.ifError("tryLock", Tuple2.of(key, milliseconds), e);
        }
        logger.afterExecute("tryLock", Tuple2.of(key, milliseconds));
        return res;
    }

    public void unlock(String key) {
        del(key);
    }
}
