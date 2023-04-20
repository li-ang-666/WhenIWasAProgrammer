package com.liang.study.nio;

import lombok.SneakyThrows;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class IOTest2 {
    @Test
    @SneakyThrows
    public void test2() {
        new File("/Users/liang/Desktop/mmap").delete();

        RandomAccessFile randomAccessFile = new RandomAccessFile("/Users/liang/Desktop/mmap", "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();
        MappedByteBuffer mmap = fileChannel
                .map(FileChannel.MapMode.READ_WRITE, 10, 10);
        System.out.println(Arrays.toString("1234567890".getBytes()));
        mmap.put("1234567890".getBytes());
    }


    @Test
    @SneakyThrows
    public void test3() {
        RandomAccessFile randomAccessFile = new RandomAccessFile("/Users/liang/Desktop/mmap", "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();
        MappedByteBuffer mmap = fileChannel
                .map(FileChannel.MapMode.READ_WRITE, 0, 100);
        while (mmap.hasRemaining()) {
            if ('\u0000' == mmap.getChar()) {
                break;
            }
        }
        System.out.println(mmap.position());
        mmap.position(mmap.position() - 2);
        System.out.println(mmap.position());
    }
}
