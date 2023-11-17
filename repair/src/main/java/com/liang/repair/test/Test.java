package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CyclicBarrier;

@Slf4j
public class Test extends ConfigHolder {
    private final static CyclicBarrier cyclicBarrier = new CyclicBarrier(5);


    @org.junit.Test
    public void test() {
        for (int i = 0; i < 3; i++) {
            new Thread(new R()).start();
        }
        System.out.println(1);
    }

    private final static class R implements Runnable {
        @Override
        @SneakyThrows
        public void run() {
            cyclicBarrier.await();
        }
    }
}
