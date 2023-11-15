package com.liang.common.service;

import com.liang.common.util.ObjectSizeCalculator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public abstract class AbstractCache<K, V> {
    private final Lock lock = new ReentrantLock();
    private final Map<K, Queue<V>> cache = new ConcurrentHashMap<>();
    private final KeySelector<K, V> keySelector;
    // buffer
    private final long bufferMax;
    private long bufferUsed;
    // cache
    private volatile int cacheMilliseconds;
    private volatile int cacheRecords;
    private volatile Thread sender;

    protected AbstractCache(int bufferMaxMb, int cacheMilliseconds, int cacheRecords, KeySelector<K, V> keySelector) {
        this.bufferUsed = 0L;
        this.bufferMax = bufferMaxMb * 1024L * 1024L;
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        this.keySelector = keySelector;
    }

    public final void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public final void enableCache(int cacheMilliseconds, int cacheRecords) {
        if (sender != null) return;
        synchronized (this) {
            if (sender != null) return;
            this.cacheMilliseconds = cacheMilliseconds;
            this.cacheRecords = cacheRecords;
            this.sender = DaemonExecutor.launch("AbstractCacheThread", () -> {
                while (true) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(cacheMilliseconds));
                    flush();
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public final void update(V... values) {
        if (values == null || values.length == 0) return;
        update(Arrays.asList(values));
    }

    @SneakyThrows
    public final void update(Collection<V> values) {
        // 拦截空值
        if (values == null || values.isEmpty()) return;
        // 保证内存不超出限制
        //long pre, after;
        //long sizeOfValues = values.stream()
        //        .map(ObjectSizeCalculator::getObjectSize)
        //        .reduce(0L, Long::sum);
        //do {
        //    pre = bufferUsed.get();
        //    after = pre + sizeOfValues;
        //} while (after > bufferMax || !bufferUsed.compareAndSet(pre, after));
        // 同一批次的写入, 不拆开
        lock.lock();
        try {
            long sizeOfValues = values.stream()
                    .map(ObjectSizeCalculator::getObjectSize)
                    .reduce(0L, Long::sum);
            while (bufferUsed + sizeOfValues > bufferMax) lock.newCondition().await();
            bufferUsed += sizeOfValues;
            for (V value : values) {
                K key = keySelector.selectKey(value);
                cache.putIfAbsent(key, new ConcurrentLinkedQueue<>());
                Queue<V> queue = cache.get(key);
                queue.add(value);
                if (queue.size() >= cacheRecords) LockSupport.unpark(sender);
            }
            if (sender == null) flush();
        } finally {
            lock.unlock();
        }
    }

    @SneakyThrows
    public final void flush() {
        if (cache.isEmpty()) return;
        lock.lock();
        try {
            if (cache.isEmpty()) return;
            for (Map.Entry<K, Queue<V>> entry : cache.entrySet()) {
                K key = entry.getKey();
                Queue<V> values = entry.getValue();
                updateImmediately(key, values);
                log.info("size: {}", values.size());
            }
            cache.clear();
            bufferUsed = 0L;
            lock.newCondition().signalAll();
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
