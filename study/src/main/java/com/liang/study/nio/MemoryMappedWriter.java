package com.liang.study.nio;

import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;


public class MemoryMappedWriter {
    private final String filePrefix;
    private final Boolean roll;

    private MappedByteBuffer mappedByteBuffer;

    private Integer i = 0;
    private Long position;
    private Long bufferUsed = 0L;
    private static final Long bufferMax = (long) Integer.MAX_VALUE;

    @SneakyThrows
    public MemoryMappedWriter(String filePrefix, boolean roll) {
        this.filePrefix = filePrefix;
        this.roll = roll;
        RandomAccessFile randomAccessFile = new RandomAccessFile(filePrefix + "-" + i, "rw");
        position = randomAccessFile.length();
        mappedByteBuffer = randomAccessFile
                .getChannel()
                .map(FileChannel.MapMode.READ_WRITE, position, bufferMax);
    }

    @SneakyThrows
    public void write(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        long length = bytes.length;

        if (bufferUsed + length > bufferMax) {
            mappedByteBuffer = new RandomAccessFile(filePrefix + "-" + (roll ? ++i : i), "rw")
                    .getChannel()
                    .map(FileChannel.MapMode.READ_WRITE, position, bufferMax);
            bufferUsed = 0L;
        }

        mappedByteBuffer.put(bytes);
        position += length;
        bufferUsed += length;
    }
}
