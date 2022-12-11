package com.liang.common.util.lambda;

@FunctionalInterface
public interface ThrowingPredicate<T> {
    boolean test(T t) throws Exception;
}
