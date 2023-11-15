package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Test extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ReentrantLock lock = new ReentrantLock();
        lock.newCondition().await();
    }
}
