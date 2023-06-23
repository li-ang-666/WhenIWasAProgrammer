package com.liang.common.service;

import java.util.*;

public abstract class MapCache<K, V> {
    protected final Map<K, List<V>> cache = new HashMap<>();
    protected final KeySelector<K, V> keySelector;
    protected int cacheMilliseconds;
    protected int cacheRecords;
    protected boolean enableCache = false;

    protected MapCache(int cacheMilliseconds, int cacheRecords, KeySelector<K, V> keySelector) {
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
            new Thread(new MapCacheSender<>(this)).start();
            enableCache = true;
        }
    }

    @SuppressWarnings("unchecked")
    public void update(V... values) {
        if (values == null || values.length == 0) {
            return;
        }
        update(Arrays.asList(values));
    }

    public void update(List<V> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        for (V value : values) {
            synchronized (cache) {
                K key = keySelector.selectKey(value);
                cache.putIfAbsent(key, new ArrayList<>());
                List<V> list = cache.get(key);
                list.add(value);
                if (enableCache && list.size() >= cacheRecords) {
                    updateImmediately(key, list);
                    cache.clear();
                }
            }
        }
        if (!enableCache && !cache.isEmpty()) {
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
