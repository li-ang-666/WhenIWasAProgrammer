package com.liang.common.service.database.template.doris;

import lombok.RequiredArgsConstructor;
import org.apache.parquet.io.PositionOutputStream;

import java.nio.ByteBuffer;

@RequiredArgsConstructor
class PositionOutputStreamBuffer extends PositionOutputStream {
    private final ByteBuffer byteBuffer;

    @Override
    public long getPos() {
        return byteBuffer.position();
    }

    @Override
    public void write(int b) {
        byteBuffer.put((byte) b);
    }
}