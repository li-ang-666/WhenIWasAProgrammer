package com.liang.common.service;

public class Timer {
    private long createTime;

    public Timer() {
        this.createTime = System.currentTimeMillis();
    }

    public long getTimeMs() {
        return System.currentTimeMillis() - createTime;
    }
}
