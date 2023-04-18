package com.liang.study.nio;

import java.io.PrintWriter;

public class IOTest {
    @org.junit.Test
    public void test() throws Exception {
        MemoryMappedWriter writer = new MemoryMappedWriter("/Users/liang/Desktop/nio", true);
        PrintWriter printWriter = new PrintWriter("/Users/liang/Desktop/io");

        long time = System.currentTimeMillis();
        for (int i = 1; i <= 100000000 / 2; i++) {
            if (i % 10000000 == 0) {
                System.out.println(i);
            }
            writer.write("我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上\n");
        }
        System.out.println("nio耗时" + (System.currentTimeMillis() - time) / 1000 + "秒");


        time = System.currentTimeMillis();
        for (int i = 1; i <= 100000000 / 2; i++) {
            if (i % 10000000 == 0) {
                System.out.println(i);
            }
            printWriter.println("我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上");
            printWriter.flush();
        }
        System.out.println("io耗时" + (System.currentTimeMillis() - time) / 1000 + "秒");
    }
}
