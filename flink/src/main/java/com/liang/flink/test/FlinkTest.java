package com.liang.flink.test;

import java.util.concurrent.Executors;

public class FlinkTest {
    public static void main(String[] args) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    System.out.println("111");
                }
            }
        };
//        Thread thread = new Thread();
//        thread.setDaemon(false);
//        thread.start();
        Executors.newSingleThreadExecutor().submit(runnable);
    }
}
