package com.liang.spark.basic;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.math.BigDecimal;

public class CountDistinct extends Aggregator<Object, Roaring64Bitmap, Long> {
    @Override
    public Roaring64Bitmap zero() {
        return new Roaring64Bitmap();
    }

    @Override
    public Roaring64Bitmap reduce(Roaring64Bitmap b, Object a) {
        try {
            b.addLong(new BigDecimal(String.valueOf(a)).longValue());
            return b;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Roaring64Bitmap merge(Roaring64Bitmap b1, Roaring64Bitmap b2) {
        b1.or(b2);
        return b2;
    }

    @Override
    public Long finish(Roaring64Bitmap reduction) {
        return reduction.getLongCardinality();
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
