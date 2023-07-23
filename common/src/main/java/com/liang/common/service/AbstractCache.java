package com.liang.common.service;

import lombok.SneakyThrows;

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class AbstractCache<K, V> {
    protected final Map<K, List<V>> cache = new HashMap<>();
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
            new Thread(new Runnable() {
                private long lastSendTime = System.currentTimeMillis();

                @Override
                @SneakyThrows(InterruptedException.class)
                public void run() {
                    while (true) {
                        TimeUnit.MILLISECONDS.sleep(100);
                        // 时间触发
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastSendTime >= cacheMilliseconds && !cache.isEmpty()) {
                            synchronized (cache) {
                                cache.forEach((key, list) -> updateImmediately(key, list));
                                cache.clear();
                            }
                            lastSendTime = currentTime;
                            // 时间触发的当轮,不判断大小
                            continue;
                        }
                        // 大小触发
                        if (!cache.isEmpty()) {
                            synchronized (cache) {
                                cache.forEach((key, list) -> {
                                    if (list.size() >= cacheRecords) updateImmediately(key, list);
                                });
                                cache.entrySet().removeIf(entry -> entry.getValue().size() >= cacheRecords);
                            }
                        }
                    }
                }
            }).start();
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

    public final void update(List<V> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        // 同一批次的写入,不拆开
        synchronized (cache) {
            for (V value : values) {
                K key = keySelector.selectKey(value);
                cache.putIfAbsent(key, new ArrayList<>());
                cache.get(key).add(value);
            }
        }
        if (!enableCache) {
            cache.forEach(this::updateImmediately);
            cache.clear();
        }
    }

    public final void flush() {
        if (!cache.isEmpty()) {
            synchronized (cache) {
                cache.forEach(this::updateImmediately);
                cache.clear();
            }
        }
    }

    protected abstract void updateImmediately(K key, List<V> values);

    @FunctionalInterface
    protected interface KeySelector<K, V> {
        K selectKey(V v);
    }
}
