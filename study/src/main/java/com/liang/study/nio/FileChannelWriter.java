package com.liang.study.nio;

import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class FileChannelWriter {
    private final static int bufferMax = 1024 * 1024 * 1024;//40M
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final ByteBuffer buffer;

    @SneakyThrows
    public FileChannelWriter(String file) {
        randomAccessFile = new RandomAccessFile(file, "rw");
        fileChannel = randomAccessFile.getChannel();
        fileChannel.position(fileChannel.size());
        buffer = ByteBuffer.allocateDirect(bufferMax);
    }

    @SneakyThrows
    public void write(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (buffer.position() + bytes.length >= bufferMax) {
            buffer.flip();
            fileChannel.write(buffer);
            buffer.clear();
        }
        buffer.put(bytes);
    }

    @SneakyThrows
    public void close() {
        buffer.flip();
        fileChannel.write(buffer);
        fileChannel.close();
        randomAccessFile.close();
    }
}
