package com.liang.flink.test;

import com.liang.flink.dto.BatchCanalBinlog;

public class FlinkTest {
    public static void main(String[] args) {
        BatchCanalBinlog batchCanalBinlog = new BatchCanalBinlog(new byte[]{
                (byte) '\n', (byte) '\t', (byte) '\r', (byte) '{', (byte) '}',
        });
        System.out.println(batchCanalBinlog);
    }
}
