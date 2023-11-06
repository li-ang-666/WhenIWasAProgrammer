package com.liang.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

public class BitmapCount implements UDF1<byte[], Long> {
    @Override
    public Long call(byte[] bytes) {
        return BitmapUtils.deserialize(bytes).getLongCardinality();
    }
}
