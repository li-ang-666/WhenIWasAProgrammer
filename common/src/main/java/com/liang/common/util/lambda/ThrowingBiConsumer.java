package com.liang.common.util.lambda;

@FunctionalInterface
public interface ThrowingBiConsumer<K, V> {
    void accept(K k, V v) throws Exception;
}
