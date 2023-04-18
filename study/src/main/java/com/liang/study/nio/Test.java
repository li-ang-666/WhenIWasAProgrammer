package com.liang.study.nio;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class Test {
    @org.junit.Test
    public void test() throws Exception {
        MMapWriter writer = MMapWriter.getInstance("/Users/liang/Desktop/nio.txt");
        writer.write("Zero copy implemented by MappedByteBuffer");
        writer.close();
    }
}
