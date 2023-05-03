package com.liang.study.test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicStampedReference;

public class Test {
    public static void main(String[] args) {
        AtomicStampedReference<String> reference = new AtomicStampedReference<>("a", 1);
    }
}

class WordCount implements Runnable {
    private final Map<Integer, AtomicStampedReference<Integer>> result = new ConcurrentHashMap<>();

    @Override
    public void run() {
        Random random = new Random();
        int i = random.nextInt(100);
        if (!result.containsKey(i)) {
            result.putIfAbsent(i, new AtomicStampedReference<>(0, 0));
        }
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