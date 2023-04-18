package com.liang.study.nio;

import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;


public class MemoryMappedWriter {
    private final String filePrefix;
    private final Boolean roll;

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private Boolean init = false;
    private Integer i = 0;
    private Long position = 0L;
    private Long bufferUsed = 0L;
    private static final Long bufferMax = (long) Integer.MAX_VALUE;

    public MemoryMappedWriter(String filePrefix, boolean roll) {
        this.filePrefix = filePrefix;
        this.roll = roll;
    }

    @SneakyThrows
    public void write(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        long length = bytes.length;
        if (!init) {
            randomAccessFile = new RandomAccessFile(filePrefix + "-" + i++ + ".txt", "rw");
            fileChannel = randomAccessFile.getChannel();
            position = randomAccessFile.length();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, bufferMax);
            init = true;
        }

        if (bufferUsed + length > bufferMax) {
            if (roll) {
                randomAccessFile = new RandomAccessFile(filePrefix + "-" + i++ + ".txt", "rw");
                fileChannel = randomAccessFile.getChannel();
                position = randomAccessFile.length();
            }
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, bufferMax);
            bufferUsed = 0L;
        }

        mappedByteBuffer.put(bytes);
        position += length;
        bufferUsed += length;
    }
}
