package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        lock.lock();
        lock.unlock();
    }
}
