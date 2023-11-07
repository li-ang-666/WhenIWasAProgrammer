package com.liang.hive.udf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Codec {

    // not support encode negative value now
    public static void encodeVarint64(long source, DataOutput out) throws IOException {
        assert source >= 0;
        short B = 128; // CHECKSTYLE IGNORE THIS LINE

        while (source >= B) {
            out.write((int) (source & (B - 1) | B));
            source = source >> 7;
        }
        out.write((int) (source & (B - 1)));
    }

    // not support decode negative value now
    public static long decodeVarint64(DataInput in) throws IOException {
        long result = 0;
        int shift = 0;
        short B = 128; // CHECKSTYLE IGNORE THIS LINE

        while (true) {
            int oneByte = in.readUnsignedByte();
            boolean isEnd = (oneByte & B) == 0;
            result = result | ((long) (oneByte & B - 1) << (shift * 7));
            if (isEnd) {
                break;
            }
            shift++;
        }

        return result;
    }
}