package com.liang.common.service;

import com.liang.common.util.ObjectSizeCalculator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public abstract class AbstractCache<K, V> {
    private final Lock lock = new ReentrantLock();
    private final Map<K, Queue<V>> cache = new ConcurrentHashMap<>();
    private final KeySelector<K, V> keySelector;
    private final AtomicBoolean enableCache = new AtomicBoolean(false);
    private final AtomicLong bufferUsed;
    private final long bufferMax;
    private int cacheMilliseconds;
    private int cacheRecords;

    protected AbstractCache(int bufferMaxMb, int cacheMilliseconds, int cacheRecords, KeySelector<K, V> keySelector) {
        this.bufferUsed = new AtomicLong(0);
        this.bufferMax = bufferMaxMb * 1024L * 1024L;
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        this.keySelector = keySelector;
    }

    public final void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public final void enableCache(int cacheMilliseconds, int cacheRecords) {
        if (enableCache.get()) return;
        lock.lock();
        try {
            if (enableCache.get()) return;
            this.cacheMilliseconds = cacheMilliseconds;
            this.cacheRecords = cacheRecords;
            DaemonExecutor.launch("AbstractCacheThread", new Runnable() {
                private long lastSendTime = System.currentTimeMillis();

                @Override
                @SneakyThrows(InterruptedException.class)
                public void run() {
                    while (true) {
                        TimeUnit.MILLISECONDS.sleep(100);
                        if (!cache.isEmpty() && System.currentTimeMillis() - lastSendTime >= cacheMilliseconds) {
                            // 时间触发
                            flush();
                            lastSendTime = System.currentTimeMillis();
                        } else if (cache.values().stream().anyMatch(queue -> queue.size() >= cacheRecords)) {
                            // 大小触发
                            flush();
                        }
                    }
                }
            });
            enableCache.set(true);
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public final void update(V... values) {
        if (values == null || values.length == 0) return;
        update(Arrays.asList(values));
    }

    public final void update(Collection<V> values) {
        // 拦截空值
        if (values == null) return;
        values.removeIf(Objects::isNull);
        if (values.isEmpty()) return;
        // 保证内存不超出限制
        long pre, after;
        long sizeOfValues = values.stream()
                .map(ObjectSizeCalculator::getObjectSize)
                .reduce(0L, Long::sum);
        do {
            pre = bufferUsed.get();
            after = pre + sizeOfValues;
        } while (after > bufferMax || !bufferUsed.compareAndSet(pre, after));
        // 同一批次的写入, 不拆开
        lock.lock();
        try {
            for (V value : values) {
                K key = keySelector.selectKey(value);
                cache.putIfAbsent(key, new ConcurrentLinkedQueue<>());
                cache.get(key).add(value);
            }
            if (!enableCache.get()) flush();
        } finally {
            lock.unlock();
        }
    }

    public final void flush() {
        if (cache.isEmpty()) return;
        lock.lock();
        try {
            if (cache.isEmpty()) return;
            long sizeOfValues = 0;
            for (Map.Entry<K, Queue<V>> entry : cache.entrySet()) {
                K key = entry.getKey();
                Queue<V> values = entry.getValue();
                // updateImmediately或许会改变value, 所以要在之前记录大小
                sizeOfValues += values.stream().map(ObjectSizeCalculator::getObjectSize).reduce(0L, Long::sum);
                updateImmediately(key, values);
            }
            cache.clear();
            bufferUsed.getAndAdd(-sizeOfValues);
        } finally {
            lock.unlock();
        }
    }

    protected abstract void updateImmediately(K key, Queue<V> values);

    @FunctionalInterface
    protected interface KeySelector<K, V> {
        K selectKey(V v);
    }
}
