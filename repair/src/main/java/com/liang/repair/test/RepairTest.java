package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            bitmap.add(i);
        }
    }
}
