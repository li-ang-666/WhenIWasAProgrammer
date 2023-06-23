package com.liang.common.service;

import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MapCacheSender<K, V> implements Runnable {
    private final MapCache<K, V> mapCache;


    public MapCacheSender(MapCache<K, V> mapCache) {
        this.mapCache = mapCache;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @SneakyThrows
    @Override
    public void run() {
        while (true) {
            TimeUnit.MILLISECONDS.sleep(mapCache.cacheMilliseconds);
            if (mapCache.cache.isEmpty()) {
                continue;
            }
            Map<K, List<V>> copyCache;
            synchronized (mapCache.cache) {
                if (mapCache.cache.isEmpty()) {
                    continue;
                }
                copyCache = new HashMap<>(mapCache.cache);
                mapCache.cache.clear();
            }
            for (Map.Entry<K, List<V>> entry : copyCache.entrySet()) {
                mapCache.updateImmediately(entry.getKey(), entry.getValue());
            }
        }
    }
}
