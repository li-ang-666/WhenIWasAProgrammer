package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        Map<Integer, Roaring64Bitmap> count2Bitmap = new HashMap<Integer, Roaring64Bitmap>();
        Integer count = count2Bitmap
                .entrySet()
                .parallelStream()
                .filter(entry -> entry.getValue().contains(2318455639L))
                .map(Map.Entry::getKey)
                .findFirst()
                .get();
    }
}
