package com.liang.study.concurrent;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * -Xms5m -Xmx5m -XX:+PrintGCDetails -XX:+PrintGCDateStamps
 */
public class ThreadLocalTest {
    public static void main(String[] args) throws Exception {
        testOK();
    }

    /**
     * OOM原因:
     * 强引用(static)的ThreadLocal对象不会被回收, key不为null, entry不会被回收
     */
    private static void testOOM1() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(Utils::oomFunc);
        System.out.println("done");
    }

    /**
     * 没有OOM原因:
     * key为null的entry, [相同线程] [下次] 执行map的set()/get()/remove()方法, entry会被回收
     */
    private static void testOK() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        for (int i = 1; i <= 7; i++) {
            executorService.execute(Utils::func);
            Thread.sleep(500);
        }
        System.out.println("done");
    }

    /**
     * key为null的entry, [相同线程] [下次] 执行map的set()/get()/remove()方法, entry会被回收
     * 多个线程, 每个线程执行一次, 每个线程的废弃entry都没被回收
     */
    private static void testOOM2() {
        ExecutorService executorService = Executors.newFixedThreadPool(7);
        for (int i = 1; i <= 7; i++) {
            executorService.execute(Utils::func);
        }
        System.out.println("done");
    }
}

class Utils {
    private static final ThreadLocal<byte[]> tl1 = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> tl2 = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> tl3 = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> tl4 = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> tl5 = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> tl6 = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> tl7 = new ThreadLocal<>();

    public static void oomFunc() {
        tl1.set(new byte[1024 * 1024]);
        tl2.set(new byte[1024 * 1024]);
        tl3.set(new byte[1024 * 1024]);
        tl4.set(new byte[1024 * 1024]);
        tl5.set(new byte[1024 * 1024]);
        tl6.set(new byte[1024 * 1024]);
        tl7.set(new byte[1024 * 1024]);
    }

    public static void func() {
        new ThreadLocal<byte[]>().set(new byte[1024 * 1024]);
    }
}

class DateUtils {
    private static final ThreadLocal<SimpleDateFormat> threadLocal = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    private DateUtils() {
    }

    public static String getDateTimeDate(Date date) {
        return threadLocal.get().format(date);
    }
}

