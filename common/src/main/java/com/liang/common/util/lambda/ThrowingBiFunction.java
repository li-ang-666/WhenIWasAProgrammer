package com.liang.common.util.lambda;

@FunctionalInterface
public interface ThrowingBiFunction<I1, I2, O> {
    O apply(I1 i1, I2 i2) throws Exception;
}
