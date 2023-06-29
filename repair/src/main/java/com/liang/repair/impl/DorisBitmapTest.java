package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.trait.Runner;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.util.List;
import java.util.function.Consumer;

public class DorisBitmapTest implements Runner {

    @Override
    public void run(String[] args) throws Exception {
        JdbcTemplate doris = new JdbcTemplate("doris");
        doris.update("INSERT INTO bitmap_test VALUES\n" +
                " (0, bitmap_empty()),\n" +
                " (1, to_bitmap(243)),\n" +
                " (2, bitmap_from_array([1,2,3,4,5,434543])),\n" +
                " (3, to_bitmap(287667876573)),\n" +
                " (4, bitmap_from_array([487667876573, 387627876573, 987567876573, 187667876573]))");

        doris.update("set global return_object_data_as_binary=true");
        List<Object> bitmaps = doris.queryForList("select btmp from bitmap_test", this::parseBitmap);
        System.out.println(bitmaps);
        doris.queryForColumnMaps("select btmp from bitmap_test");
//        List<Object> lst = doris.queryForList("select btmp from bitmap_test", rs -> rs.getObject(1));
//        for (Object o : lst) {
//            System.out.println(o.getClass());
//        }
    }

    private Object parseBitmap(ResultSet resultSet) throws Exception {
        byte[] bytes = resultSet.getBytes(1);
        RoaringBitmap bitmap32 = new RoaringBitmap();
        // Only Roaring64NavigableMap can work, Roaring64Bitmap can't work!!!
        Roaring64NavigableMap bitmap64 = new Roaring64NavigableMap();
        switch (bytes[0]) {
            case 0: // for empty bitmap
                break;
            case 1: // for only 1 element in bitmap32
                bitmap32.add(ByteBuffer.wrap(bytes, 1, bytes.length - 1)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt());
                break;
            case 2: // for more than 1 element in bitmap32
                bitmap32.deserialize(ByteBuffer.wrap(bytes, 1, bytes.length - 1));
                break;
            case 3: // for only 1 element in bitmap64
                bitmap64.add(ByteBuffer.wrap(bytes, 1, bytes.length - 1)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getLong());
                break;
            case 4: // for more than 1 element in bitmap64
                Object[] tuple2 = decodeVarInt64(bytes);
                int offset = (int) tuple2[1];
                int newLen = 8 + bytes.length - offset;

                try (ByteArrayOutputStream baos = new ByteArrayOutputStream(newLen);
                     DataOutputStream dos = new DataOutputStream(baos)) {
                    dos.write((byte[]) tuple2[0]);
                    dos.write(bytes, offset, bytes.length - offset);
                    dos.flush();
                    try (DataInputStream dis = new DataInputStream(
                            new ByteArrayInputStream(baos.toByteArray()))) {
                        bitmap64.deserializePortable(dis);
                    }
                }
                break;
        }
        if (bytes[0] <= 2) {
            bitmap32.forEach((Consumer<Integer>) bitmap64::addInt);
        }
        return bitmap64;
    }

    private Object[] decodeVarInt64(byte[] bt) { // nolint
        long result = 0;
        int shift = 0;
        short B = 128;
        int idx = 1;
        for (; ; ) {
            short readByte = bt[idx];
            idx++;
            boolean isEnd = (readByte & B) == 0;
            result |= (long) (readByte & (B - 1)) << (shift * 7);
            if (isEnd) {
                break;
            }
            shift++;
        }
        byte[] bytes = new byte[8];
        for (int i = 0; i < bytes.length; i++) {
            // LITTLE_ENDIAN
            bytes[i] = (byte) (result >> 8 * i);
        }
        return new Object[]{bytes, idx};
    }
}