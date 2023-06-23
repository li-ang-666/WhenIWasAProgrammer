package com.liang.common.service;

import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ListCacheSender<E> implements Runnable {
    private final ListCache<E> listCache;


    public ListCacheSender(ListCache<E> listCache) {
        this.listCache = listCache;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @SneakyThrows
    @Override
    public void run() {
        while (true) {
            TimeUnit.MILLISECONDS.sleep(listCache.cacheMilliseconds);
            if (listCache.cache.isEmpty()) {
                continue;
            }
            List<E> copyCache;
            synchronized (listCache.cache) {
                if (listCache.cache.isEmpty()) {
                    continue;
                }
                copyCache = new ArrayList<>(listCache.cache);
                listCache.cache.clear();
            }
            listCache.updateImmediately(copyCache);
        }
    }
}
