package com.liang.common.util.lambda;

import lombok.extern.slf4j.Slf4j;

import java.util.function.*;

/**
 * 如果"函数式接口"未抛出Exception
 * 则说明该方法的实现也不能抛出Exception
 * 也就是说方法体或者函数体(也就是{}里面的内容)必须做好try-catch
 */
@Slf4j
public class LambdaExceptionEraser {
    private LambdaExceptionEraser() {
    }

    public static <E> Consumer<E> consumerEraser(ThrowingConsumer<E> throwingConsumer) {
        return elem -> {
            try {
                throwingConsumer.accept(elem);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <K, V> BiConsumer<K, V> biConsumerEraser(ThrowingBiConsumer<K, V> throwingBiConsumer) {
        return (k, v) -> {
            try {
                throwingBiConsumer.accept(k, v);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }


    public static <I, O> Function<I, O> functionEraser(ThrowingFunction<I, O> throwingFunction) {
        return elem -> {
            try {
                return throwingFunction.apply(elem);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <I1, I2, O> BiFunction<I1, I2, O> biFunctionEraser(ThrowingBiFunction<I1, I2, O> throwingBiFunction) {
        return (e1, e2) -> {
            try {
                return throwingBiFunction.apply(e1, e2);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T> BinaryOperator<T> biMergeEraser(ThrowingBinaryOperator<T> throwingBinaryOperator) {
        return (e1, e2) -> {
            try {
                return throwingBinaryOperator.apply(e1, e2);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T> Predicate<T> predicateEraser(ThrowingPredicate<T> throwingPredicate) {
        return elem -> {
            try {
                return throwingPredicate.test(elem);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
