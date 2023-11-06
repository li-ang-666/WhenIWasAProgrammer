package com.liang.spark.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

public class CollectBitmap extends Aggregator<Long, Roaring64Bitmap, byte[]> {

    @Override
    public Roaring64Bitmap zero() {
        return new Roaring64Bitmap();
    }

    @Override
    public Roaring64Bitmap reduce(Roaring64Bitmap b, Long a) {
        b.addLong(a);
        return b;
    }

    @Override
    public Roaring64Bitmap merge(Roaring64Bitmap b1, Roaring64Bitmap b2) {
        b1.or(b2);
        return b1;
    }

    @Override
    public byte[] finish(Roaring64Bitmap reduction) {
        return BitmapUtils.serialize(reduction);
    }

    @Override
    public Encoder<Roaring64Bitmap> bufferEncoder() {
        return Encoders.javaSerialization(Roaring64Bitmap.class);
    }

    @Override
    public Encoder<byte[]> outputEncoder() {
        return Encoders.BINARY();
    }
}
