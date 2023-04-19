package com.liang.study.nio;

import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class FileChannelWriter {
    private final static int size = 1024 * 1024 * 1024;

    private final FileChannel fileChannel;
    private final ByteBuffer buffer;

    @SneakyThrows
    public FileChannelWriter(String file) {
        fileChannel = new RandomAccessFile(file, "rw").getChannel();
        fileChannel.position(fileChannel.size());
        buffer = ByteBuffer.allocate(size);
    }

    @SneakyThrows
    public void write(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (buffer.position() + bytes.length > size) {
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
        fileChannel.force(false);
        fileChannel.close();
    }
}
