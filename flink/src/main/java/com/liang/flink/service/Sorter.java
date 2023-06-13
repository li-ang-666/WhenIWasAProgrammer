package com.liang.flink.service;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class Sorter {
    private final Map<String, AtomicI> map = new LinkedHashMap<>();

    public void add(String key, Integer count) {
        log.info("----------------key: {}, count: {}", key, count);
        if (map.containsKey(key)) {
            map.get(key).add(count);
        } else {
            map.put(key, new AtomicI(count));
        }
    }

    public String getMaxCountKey() {
        String key = null;
        int value = Integer.MIN_VALUE;
        for (Map.Entry<String, AtomicI> entry : map.entrySet()) {
            //按照先后顺序,后出现的必须大于,才能上位
            if (entry.getValue().get() > value) {
                key = entry.getKey();
                value = entry.getValue().get();
            }
        }
        return key;
    }

    private static class AtomicI {
        int i;

        private AtomicI(int i) {
            this.i = i;
        }

        private void add(int i) {
            this.i += i;
        }

        private int get() {
            return this.i;
        }
    }
}
