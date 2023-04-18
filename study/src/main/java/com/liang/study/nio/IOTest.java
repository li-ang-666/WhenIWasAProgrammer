package com.liang.study.nio;

import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

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

    @Test
    public void head() {
        //512m
        long length = 1L << 29;
        //4g
        long _4G = 1L << 32;
        long cur = 0L;
        try {
            MappedByteBuffer mappedByteBuffer;
            Random random = new Random();
            while (cur < _4G) {
                mappedByteBuffer = new RandomAccessFile("/Users/liang/Desktop/nio", "rw").getChannel()
                        .map(FileChannel.MapMode.READ_WRITE, cur, length);
                IntBuffer intBuffer = mappedByteBuffer.asIntBuffer();
                while (intBuffer.position() < intBuffer.capacity()) {
                    intBuffer.put(random.nextInt());
                }
                cur += length;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
