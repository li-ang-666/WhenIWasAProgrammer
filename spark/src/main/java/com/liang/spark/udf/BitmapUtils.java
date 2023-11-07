package com.liang.spark.udf;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

@UtilityClass
public class BitmapUtils {
    @SneakyThrows
    public static byte[] serialize(Roaring64Bitmap roaring64Bitmap) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        roaring64Bitmap.serialize(dos);
        dos.close();
        return bos.toByteArray();
    }

    @SneakyThrows
    public static Roaring64Bitmap deserialize(byte[] bytes) {
        Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        roaring64Bitmap.deserialize(in);
        in.close();
        return roaring64Bitmap;
    }
}