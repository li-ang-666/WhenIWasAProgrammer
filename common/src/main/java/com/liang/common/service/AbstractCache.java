package com.liang.common.service;

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class AbstractCache<K, V> {
    protected final Map<K, List<V>> cache = new HashMap<>();
    protected final KeySelector<K, V> keySelector;
    protected int cacheMilliseconds;
    protected int cacheRecords;
    protected volatile boolean enableCache = false;

    protected AbstractCache(int cacheMilliseconds, int cacheRecords, KeySelector<K, V> keySelector) {
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        this.keySelector = keySelector;
    }

    public void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public void enableCache(int cacheMilliseconds, int cacheRecords) {
        if (!enableCache) {
            this.cacheMilliseconds = cacheMilliseconds;
            this.cacheRecords = cacheRecords;
            new Thread(() -> {
                while (true) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(cacheMilliseconds);
                    } catch (Exception ignore) {
                    }
                    if (cache.isEmpty()) {
                        continue;
                    }
                    Map<K, List<V>> copyCache;
                    synchronized (cache) {
                        if (cache.isEmpty()) {
                            continue;
                        }
                        copyCache = new HashMap<>(cache);
                        cache.clear();
                    }
                    for (Map.Entry<K, List<V>> entry : copyCache.entrySet()) {
                        updateImmediately(entry.getKey(), entry.getValue());
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
        Map<K, List<V>> copyCache = new HashMap<>();
        synchronized (cache) {
            for (V value : values) {
                K key = keySelector.selectKey(value);
                cache.putIfAbsent(key, new ArrayList<>());
                List<V> list = cache.get(key);
                list.add(value);
            }
            if (enableCache) {
                for (Map.Entry<K, List<V>> entry : cache.entrySet()) {
                    K key = entry.getKey();
                    List<V> value = entry.getValue();
                    if (value.size() >= cacheRecords) {
                        copyCache.put(key, value);
                        cache.remove(key);
                    }
                }
            }
        }
        for (Map.Entry<K, List<V>> entry : copyCache.entrySet()) {
            K key = entry.getKey();
            List<V> value = entry.getValue();
            updateImmediately(key, value);
        }
        if (!enableCache) {
            for (Map.Entry<K, List<V>> entry : cache.entrySet()) {
                updateImmediately(entry.getKey(), entry.getValue());
            }
            cache.clear();
        }
    }

    public final void flush() {
        if (cache.isEmpty()) {
            return;
        }
        synchronized (cache) {
            if (cache.isEmpty()) {
                return;
            }
            for (Map.Entry<K, List<V>> entry : cache.entrySet()) {
                updateImmediately(entry.getKey(), entry.getValue());
            }
            cache.clear();
        }
    }

    protected abstract void updateImmediately(K key, List<V> values);

    @FunctionalInterface
    protected interface KeySelector<K, V> {
        K selectKey(V v);
    }
}
