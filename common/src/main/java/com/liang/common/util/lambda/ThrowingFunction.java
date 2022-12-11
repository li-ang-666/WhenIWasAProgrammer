package com.liang.common.util.lambda;

@FunctionalInterface
public interface ThrowingFunction<I, O> {
    O apply(I i) throws Exception;
}
