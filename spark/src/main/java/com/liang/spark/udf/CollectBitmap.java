package com.liang.spark.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

public class CollectBitmap extends Aggregator<Long, byte[], byte[]> {
    @Override
    public byte[] zero() {
        return BitmapUtils.serialize(new Roaring64Bitmap());
    }

    @Override
    public byte[] reduce(byte[] b, Long a) {
        Roaring64Bitmap bitmap = BitmapUtils.deserialize(b);
        bitmap.addLong(a);
        return BitmapUtils.serialize(bitmap);
    }

    @Override
    public byte[] merge(byte[] b1, byte[] b2) {
        Roaring64Bitmap bitmap1 = BitmapUtils.deserialize(b1);
        Roaring64Bitmap bitmap2 = BitmapUtils.deserialize(b2);
        bitmap1.or(bitmap2);
        return BitmapUtils.serialize(bitmap1);
    }

    @Override
    public byte[] finish(byte[] reduction) {
        return reduction;
    }

    @Override
    public Encoder<byte[]> bufferEncoder() {
        return Encoders.BINARY();
    }

    @Override
    public Encoder<byte[]> outputEncoder() {
        return Encoders.BINARY();
    }
}
