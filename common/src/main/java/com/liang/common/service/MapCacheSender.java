package com.liang.common.service;

import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MapCacheSender<K, V> implements Runnable {
    private final AbstractCache<K, V> abstractCache;


    public MapCacheSender(AbstractCache<K, V> abstractCache) {
        this.abstractCache = abstractCache;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @SneakyThrows
    @Override
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
                abstractCache.updateImmediately(entry.getKey(), entry.getValue());
            }
        }
    }
}
