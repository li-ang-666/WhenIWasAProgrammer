package com.liang.common.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractCache<K, V> {
    protected final Map<K, Queue<V>> cache = new ConcurrentHashMap<>();
    protected final KeySelector<K, V> keySelector;
    protected int cacheMilliseconds;
    protected int cacheRecords;
    protected boolean enableCache = false;

    protected AbstractCache(int cacheMilliseconds, int cacheRecords, KeySelector<K, V> keySelector) {
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        this.keySelector = keySelector;
    }

    public final void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public final void enableCache(int cacheMilliseconds, int cacheRecords) {
        if (!enableCache) {
            this.cacheMilliseconds = cacheMilliseconds;
            this.cacheRecords = cacheRecords;
            DaemonExecutor.launch("AbstractCacheThread", new Runnable() {
                private long lastSendTime = System.currentTimeMillis();

                @Override
                @SneakyThrows(InterruptedException.class)
                public void run() {
                    while (true) {
                        TimeUnit.MILLISECONDS.sleep(10);
                        // 时间触发
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastSendTime >= cacheMilliseconds && !cache.isEmpty()) {
                            synchronized (cache) {
                                // 遍历, 清空
                                cache.forEach((key, queue) -> updateImmediately(key, queue));
                                cache.clear();
                            }
                            lastSendTime = currentTime;
                            // 时间触发的当轮,不判断大小
                            continue;
                        }
                        // 大小触发
                        long count = cache.values().stream().filter(queue -> queue.size() >= cacheRecords).count();
                        if (count > 0) {
                            synchronized (cache) {
                                // 遍历, 剔除
                                cache.forEach((key, queue) -> {
                                    if (queue.size() >= cacheRecords) updateImmediately(key, queue);
                                });
                                cache.entrySet().removeIf(entry -> entry.getValue().size() >= cacheRecords);
                            }
                        }
                    }
                }
            });
            enableCache = true;
        }
    }

    @SuppressWarnings("unchecked")
    public final void update(V... values) {
        if (values == null || values.length == 0) {
            return;
        }
        update(Arrays.asList(values));
    }

    public final void update(Collection<V> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        // 同一批次的写入,不拆开
        synchronized (cache) {
            for (V value : values) {
                if (value == null) {
                    continue;
                }
                K key = keySelector.selectKey(value);
                cache.putIfAbsent(key, new ConcurrentLinkedQueue<>());
                cache.get(key).add(value);
            }
        }
        if (!enableCache) {
            // 遍历, 清空
            cache.forEach(this::updateImmediately);
            cache.clear();
        }
    }

    public final void flush() {
        if (!cache.isEmpty()) {
            synchronized (cache) {
                // 遍历, 清空
                cache.forEach(this::updateImmediately);
                cache.clear();
            }
        }
    }

    protected abstract void updateImmediately(K key, Queue<V> values);

    @FunctionalInterface
    protected interface KeySelector<K, V> {
        K selectKey(V v);
    }
}
