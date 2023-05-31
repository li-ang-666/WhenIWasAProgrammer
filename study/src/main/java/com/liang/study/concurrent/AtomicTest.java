package com.liang.study.concurrent;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicStampedReference;

public class AtomicTest {
    public static void main(String[] args) {

    }
}

/**
 * 多线程情况下，HashMap线程不安全，可能存在:
 * 线程A、B都判断需要尾插，A先插入了key1，B又插入key2(key1与key2的hash值相同)，造成A线程的结果被覆盖
 * (LinkedList同理,需要ConcurrentLinkedQueue)
 * <p>
 * ConcurrentHashMap避免了以上问，通过锁Node的方式
 * 但是用ConcurrentHashMap实现word count,依旧会出现count++的线程不安全
 * 需要使用AtomicReference
 */
class WordCount implements Runnable {
    private final Map<Integer, AtomicStampedReference<Integer>> result = new ConcurrentHashMap<>();

    @Override
    public void run() {
        Random random = new Random();
        int i = random.nextInt(100);
        if (!result.containsKey(i)) {
            result.putIfAbsent(i, new AtomicStampedReference<>(0, 0));
        }
        //这里不用Stamped也可以,因为word count不在乎顺序性,只在乎是不是count++成功
        AtomicStampedReference<Integer> atomicReference = result.get(i);
        int preValue, afterValue;
        int preVersion, afterVersion;
        do {
            preValue = atomicReference.getReference();
            afterValue = preValue + 1;

            preVersion = atomicReference.getStamp();
            afterVersion = preVersion + 1;
        } while (!atomicReference.compareAndSet(preValue, afterValue, preVersion, afterVersion));
    }
}
