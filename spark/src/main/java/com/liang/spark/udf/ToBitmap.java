package com.liang.spark.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.roaringbitmap.longlong.Roaring64Bitmap;

public class ToBitmap implements UDF1<Long, byte[]> {

    @Override
    public byte[] call(Long aLong) {
        return BitmapUtils.serialize(Roaring64Bitmap.bitmapOf(aLong));
    }
}
