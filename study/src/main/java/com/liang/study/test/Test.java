package com.liang.study.test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class Test {
    public static void main(String[] args) throws Exception {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println(f());
            }

            private boolean f() {
                LockSupport.park();
                return Thread.interrupted();
            }
        });
        thread.start();
        TimeUnit.SECONDS.sleep(2);
        thread.interrupt();
        TimeUnit.SECONDS.sleep(2);
        new ReentrantLock().newCondition().awaitUninterruptibly();
    }
}
