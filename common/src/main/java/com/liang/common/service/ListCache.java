package com.liang.common.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class ListCache<E> {
    protected final List<E> cache = new ArrayList<>();
    protected int cacheMilliseconds;
    protected int cacheRecords;
    protected boolean enableCache = false;

    protected ListCache(int cacheMilliseconds, int cacheRecords) {
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
    }

    public void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public void enableCache(int cacheMilliseconds, int cacheRecords) {
        if (!enableCache) {
            this.cacheMilliseconds = cacheMilliseconds;
            this.cacheRecords = cacheRecords;
            new Thread(new ListCacheSender<>(this)).start();
            enableCache = true;
        }
    }

    @SuppressWarnings("unchecked")
    public void update(E... elements) {
        if (elements == null || elements.length == 0) {
            return;
        }
        update(Arrays.asList(elements));
    }

    public void update(List<E> elements) {
        if (elements == null || elements.isEmpty()) {
            return;
        }
        for (E e : elements) {
            synchronized (cache) {
                cache.add(e);
                if (enableCache && cache.size() >= cacheRecords) {
                    updateImmediately(cache);
                    cache.clear();
                }
            }
        }
        if (!enableCache && !cache.isEmpty()) {
            updateImmediately(cache);
            cache.clear();
        }
    }

    protected abstract void updateImmediately(List<E> elements);
}
