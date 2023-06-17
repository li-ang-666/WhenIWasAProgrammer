package com.liang.common.service.database.template;

import com.liang.common.service.database.holder.JedisPoolHolder;
import com.liang.common.service.database.template.inner.TemplateLogger;
import lombok.extern.slf4j.Slf4j;
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
        logger.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            String res = jedis.get(key);
            logger.afterExecute("get", key);
            return res;
        } catch (Exception e) {
            logger.ifError("get", key, e);
            return null;
        }
    }

    public void set(String key, String value) {
        logger.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            jedis.set(key, value);
            logger.afterExecute("set", key + " -> " + value);
        } catch (Exception e) {
            logger.ifError("set", key + " -> " + value, e);
        }
    }

    public void del(String key) {
        logger.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            jedis.del(key);
            logger.afterExecute("del", key);
        } catch (Exception e) {
            logger.ifError("del", key, e);
        }
    }

    /**
     * <p>127.0.0.1:6379> HSCAN ${key} ${cursor} [MATCH pattern] [COUNT count]
     * <p>HSCAN 命令每次被调用之后, 都会向用户返回一个新的游标, 用户在下次迭代时需要使用这个新游标作为 HSCAN 命令的游标参数, 以此来延续之前的迭代
     * <p>当 SCAN 命令的游标参数被设置为 0 时, 服务器将开始一次新的迭代, 而当服务器向用户返回值为 0 的游标时, 表示迭代已结束
     */

    public Map<String, String> hScan(String key) {
        logger.beforeExecute();
        Map<String, String> result = new HashMap<>();
        try (Jedis jedis = pool.getResource()) {
            String cursor = ScanParams.SCAN_POINTER_START; //其实就是 "0"
            ScanParams scanParams = new ScanParams().match("*").count(100);
            do {
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(key, cursor, scanParams);
                cursor = scanResult.getCursor();
                scanResult.getResult().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
            } while (!"0".equals(cursor));
            logger.afterExecute("hScan", key);
            return result;
        } catch (Exception e) {
            logger.ifError("hScan", key, e);
            return result;
        }
    }

    public List<String> scan() {
        logger.beforeExecute();
        List<String> result = new ArrayList<>();
        try (Jedis jedis = pool.getResource()) {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match("*").count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                result.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));
            logger.afterExecute("scan", "");
            return result;
        } catch (Exception e) {
            logger.ifError("scan", "", e);
            return result;
        }
    }

    public boolean tryLock(String key) {
        logger.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            Long reply = jedis.setnx(key, "lock");
            logger.afterExecute("tryLock", key);
            return reply == 1;
        } catch (Exception e) {
            logger.ifError("tryLock", key, e);
            return false;
        }
    }

    public void unlock(String key) {
        del(key);
    }
}
