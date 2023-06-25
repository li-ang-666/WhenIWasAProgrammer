package com.liang.common.service;

import lombok.SneakyThrows;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class AbstractCache<K, V> implements Serializable {
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

    public void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public void enableCache(int cacheMilliseconds, int cacheRecords) {
        if (!enableCache) {
            this.cacheMilliseconds = cacheMilliseconds;
            this.cacheRecords = cacheRecords;
            enableCache = true;
            new Thread(new Sender<>(this)).start();
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
        for (V value : values) {
            Tuple2<K, List<V>> kv = null;
            synchronized (cache) {
                K key = keySelector.selectKey(value);
                cache.putIfAbsent(key, new ArrayList<>());
                List<V> list = cache.get(key);
                list.add(value);
                if (enableCache && list.size() >= cacheRecords) {
                    kv = Tuple2.of(key, list);
                    cache.remove(key);
                }
            }
            if (kv != null) {
                updateImmediately(kv.f0, kv.f1);
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

    private static class Sender<K, V> implements Runnable {
        private final AbstractCache<K, V> abstractCache;
        private final AbstractCache<K, V> copyAbstractCache;

        public Sender(AbstractCache<K, V> abstractCache) {
            this.abstractCache = abstractCache;
            this.copyAbstractCache = SerializationUtils.clone(abstractCache);
        }

        @Override
        @SneakyThrows
        @SuppressWarnings("InfiniteLoopStatement")
        public void run() {
            while (true) {
                TimeUnit.MILLISECONDS.sleep(abstractCache.cacheMilliseconds);
                if (abstractCache.cache.isEmpty()) {
                    continue;
                }
                Map<K, List<V>> copyCache;
                synchronized (abstractCache.cache) {
                    if (abstractCache.cache.isEmpty()) {
                        continue;
                    }
                    copyCache = new HashMap<>(abstractCache.cache);
                    abstractCache.cache.clear();
                }
                for (Map.Entry<K, List<V>> entry : copyCache.entrySet()) {
                    copyAbstractCache.updateImmediately(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}
