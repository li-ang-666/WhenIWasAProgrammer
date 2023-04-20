package com.liang.study.nio;

import lombok.SneakyThrows;
import sun.nio.ch.DirectBuffer;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;


public class MemoryMappedWriter {
    private static final int bufferMax = 1024 * 1024 * 1024;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private MappedByteBuffer mmap;
    private long position;

    @SneakyThrows
    public MemoryMappedWriter(String file) {
        randomAccessFile = new RandomAccessFile(file, "rw");
        position = Math.max(randomAccessFile.length() - bufferMax, 0L);
        fileChannel = randomAccessFile.getChannel();
        mmap = fileChannel
                .map(FileChannel.MapMode.READ_WRITE, position, bufferMax);
        while (mmap.hasRemaining()) {
            if ('\u0000' == mmap.getChar()) break;
        }
        mmap.position(mmap.position() - 2);
        position += mmap.position();
    }

    @SneakyThrows
    public void write(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;

        if (mmap.position() + length >= bufferMax) {
            System.out.println("mmap重置");
            randomAccessFile.setLength(position);
            ((DirectBuffer) mmap).cleaner().clean();
            mmap = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, position, bufferMax);
        }

        mmap.put(bytes);
        position += length;
    }

    @SneakyThrows
    public void close() {
        randomAccessFile.setLength(position);
        ((DirectBuffer) mmap).cleaner().clean();
        fileChannel.close();
        randomAccessFile.close();
    }
}
