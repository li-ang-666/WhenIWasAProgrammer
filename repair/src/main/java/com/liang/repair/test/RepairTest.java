package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        long i = 0L;
        while (i < 500000000L) {
            bitmap.add(i++);
        }
        System.out.println(bitmap.getSizeInBytes());
    }
}
