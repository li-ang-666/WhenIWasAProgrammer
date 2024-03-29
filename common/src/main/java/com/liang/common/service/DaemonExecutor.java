package com.liang.common.service;

import lombok.experimental.UtilityClass;

@UtilityClass
public class DaemonExecutor {
    public static Thread launch(String name, Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName(name);
        thread.start();
        return thread;
    }
}
