package com.liang.study.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

public class Test {
    private final static ThreadLocal<int[]> THREAD_LOCAL = ThreadLocal.withInitial(() -> new int[Integer.MAX_VALUE]);

    public static void main(String[] args) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(Integer.MAX_VALUE);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            executorService.submit(() -> {
                THREAD_LOCAL.get();
                LockSupport.park();
            });
            System.out.println(i);
        }
        System.out.println("done");
    }
}
