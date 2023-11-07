package com.liang.spark.basic;

import lombok.SneakyThrows;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

public class CountDistinct extends Aggregator<String, Roaring64Bitmap, Long> {
    @Override
    public Roaring64Bitmap zero() {
        return new Roaring64Bitmap();
    }

    @Override
    @SneakyThrows(NumberFormatException.class)
    public Roaring64Bitmap reduce(Roaring64Bitmap buffer, String elem) {
        if (elem == null) {
            return buffer;
        }
        buffer.add(Long.parseLong(elem));
        return buffer;
    }

    @Override
    public Roaring64Bitmap merge(Roaring64Bitmap buffer1, Roaring64Bitmap buffer2) {
        buffer1.or(buffer2);
        return buffer1;
    }

    @Override
    public Long finish(Roaring64Bitmap buffer) {
        return buffer.getLongCardinality();
    }

    @Override
    public Encoder<Roaring64Bitmap> bufferEncoder() {
        return Encoders.javaSerialization(Roaring64Bitmap.class);
    }

    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}
