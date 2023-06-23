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

    public final void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public final void enableCache(int cacheMilliseconds, int cacheRecords) {
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        if (!enableCache) {
            new Thread(new ListCacheSender<>(this)).start();
            enableCache = true;
        }
    }

    @SafeVarargs
    public final void update(E... es) {
        if (es == null || es.length == 0) {
            return;
        }
        update(Arrays.asList(es));
    }

    public final void update(List<E> es) {
        if (es == null || es.isEmpty()) {
            return;
        }
        for (E e : es) {
            synchronized (cache) {
                cache.add(e);
                if (enableCache && cache.size() >= cacheRecords) {
                    updateImmediately(cache);
                    cache.clear();
                }
            }
        }
        if (!enableCache && !es.isEmpty()) {
            updateImmediately(cache);
            cache.clear();
        }
    }

    protected abstract void updateImmediately(List<E> es);
}
