package com.liang.common.service;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public abstract class AbstractCache<K, V> {
    private final Lock lock = new ReentrantLock(true);
    private final Map<K, Collection<V>> cache = new ConcurrentHashMap<>();
    private final KeySelector<K, V> keySelector;
    // cache
    private volatile int cacheMilliseconds;
    private volatile int cacheRecords;
    private volatile Thread sender;

    protected AbstractCache(int cacheMilliseconds, int cacheRecords, KeySelector<K, V> keySelector) {
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        this.keySelector = keySelector;
    }

    public final void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public final void enableCache(int cacheMilliseconds, int cacheRecords) {
        if (sender != null) return;
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        sender = DaemonExecutor.launch("AbstractCacheThread", () -> {
            while (true) {
                LockSupport.parkUntil(System.currentTimeMillis() + this.cacheMilliseconds);
                flush();
            }
        });
    }

    @SuppressWarnings("unchecked")
    public final void update(V... values) {
        if (values == null || values.length == 0) return;
        update(Arrays.asList(values));
    }

    public final void update(Collection<V> values) {
        // 拦截空值
        if (values == null || values.isEmpty()) return;
        lock.lock();
        try {
            // 同一批次的写入, 不拆开
            for (V value : values) {
                if (value == null) continue;
                K key = keySelector.selectKey(value);
                cache.putIfAbsent(key, new ConcurrentLinkedQueue<>());
                Collection<V> buffer = cache.get(key);
                buffer.add(value);
                // 有分区达到条数阈值, 唤醒sender
                if (buffer.size() >= cacheRecords) LockSupport.unpark(sender);
            }
            if (sender == null) flush();
        } finally {
            lock.unlock();
        }
    }

    public final void flush() {
        lock.lock();
        try {
            for (Map.Entry<K, Collection<V>> entry : cache.entrySet()) {
                Collection<V> values = entry.getValue();
                if (values.isEmpty()) continue;
                K key = entry.getKey();
                updateImmediately(key, values);
                values.clear();
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract void updateImmediately(K key, Collection<V> values);

    @FunctionalInterface
    protected interface KeySelector<K, V> {
        K selectKey(V v);
    }
}
