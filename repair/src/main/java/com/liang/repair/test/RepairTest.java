package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        log.info("1");
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (int i = 0; i < 400_000_000; i++) {
            bitmap.add(i);
        }
        log.info("1");
        bitmap.forEach(i -> {
        });
        log.info("1");
    }
}
