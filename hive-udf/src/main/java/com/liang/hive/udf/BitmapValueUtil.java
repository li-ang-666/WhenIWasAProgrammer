package com.liang.hive.udf;

import java.io.*;

public class BitmapValueUtil {
    public static byte[] serializeToBytes(BitmapValue bitmapValue) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bitmapValue.serialize(dos);
        dos.close();
        return bos.toByteArray();
    }

    public static BitmapValue deserializeToBitmap(byte[] bytes) throws IOException {
        BitmapValue bitmapValue = new BitmapValue();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        bitmapValue.deserialize(in);
        in.close();
        return bitmapValue;
    }
}