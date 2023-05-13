package com.liang.study.concurrent;

import java.util.concurrent.locks.ReentrantLock;

public class LockTest {
    public static void main(String[] args) throws Exception {
        ReentrantLock lock = new ReentrantLock(true);
        lock.wait();
        lock.lock();
        lock.unlock();
    }
}
