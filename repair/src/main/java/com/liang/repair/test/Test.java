package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class Test extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        Thread thread = Thread.currentThread();
        thread.interrupt();
        System.out.println(thread.isInterrupted());
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception ignore) {
        }
        System.out.println(thread.isInterrupted());
        System.out.println(1);


        LockSupport.unpark(thread);
        LockSupport.park();
        System.out.println(2);
        new Thread().interrupt();
    }
}
