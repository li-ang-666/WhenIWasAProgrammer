package com.liang.common.service.database.template.doris;

import lombok.RequiredArgsConstructor;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.nio.ByteBuffer;

@RequiredArgsConstructor
class OutputFileBuffer implements OutputFile {
    private final ByteBuffer byteBuffer;

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return new PositionOutputStreamBuffer(byteBuffer);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @RequiredArgsConstructor
    private static final class PositionOutputStreamBuffer extends PositionOutputStream {
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
}