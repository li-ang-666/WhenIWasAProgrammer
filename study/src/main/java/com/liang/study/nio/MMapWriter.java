package com.liang.study.nio;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class MMapWriter {
    private RandomAccessFile randomAccessFile = null;
    private FileChannel fileChannel = null;
    private long position = 0;

    private MMapWriter() {
    }

    public static MMapWriter getInstance(String filePath) {
        MMapWriter writer;
        try {
            writer = new MMapWriter();
            writer.randomAccessFile = new RandomAccessFile(filePath, "rw");
            writer.fileChannel = writer.randomAccessFile.getChannel();
            writer.position = writer.randomAccessFile.length();
            return writer;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void write(String content) {
        try {
            content += "\n";
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            int length = bytes.length;
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, length);
            mappedByteBuffer.put(bytes);
            position += length;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
