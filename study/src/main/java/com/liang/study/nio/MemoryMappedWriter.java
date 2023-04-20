package com.liang.study.nio;

import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;


public class MemoryMappedWriter {
    private final String filePrefix;
    private final Boolean roll;
    private Integer i = 0;

    private MappedByteBuffer mappedByteBuffer;

    private static final Long bufferMax = 1024L * 1024 * 1024;

    @SneakyThrows
    public MemoryMappedWriter(String filePrefix, boolean roll) {
        this.filePrefix = filePrefix;
        this.roll = roll;
        FileChannel fileChannel = new RandomAccessFile(filePrefix + "-" + i, "rw").getChannel();
        mappedByteBuffer = fileChannel
                .map(FileChannel.MapMode.READ_WRITE, fileChannel.size(), bufferMax);
    }

    @SneakyThrows
    public void write(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

        if (mappedByteBuffer.position() + bytes.length >= bufferMax) {
            FileChannel fileChannel = new RandomAccessFile(filePrefix + "-" + (roll ? ++i : i), "rw").getChannel();
            mappedByteBuffer = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, fileChannel.size(), bufferMax);
        }

        mappedByteBuffer.put(bytes);
    }
}
