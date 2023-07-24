package com.liang.repair.impl;

import cn.hutool.core.lang.Snowflake;
import com.liang.repair.service.ConfigHolder;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SnowIdTest extends ConfigHolder {

    public static void main(String[] args) {
        Set<Long> set = ConcurrentHashMap.newKeySet();
        Snowflake snowflake1 = new Snowflake(1);
        Snowflake snowflake2 = new Snowflake(2);
        Snowflake snowflake3 = new Snowflake(3);
        Snowflake snowflake4 = new Snowflake(4);
        Snowflake snowflake5 = new Snowflake(5);
        new Thread(() -> {
            while (true) {
                long l = snowflake1.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l: {}", l);
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                long l = snowflake2.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l: {}", l);
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                long l = snowflake3.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l: {}", l);
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                long l = snowflake4.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l: {}", l);
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                long l = snowflake5.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l: {}", l);
                }
            }
        }).start();
    }
}
