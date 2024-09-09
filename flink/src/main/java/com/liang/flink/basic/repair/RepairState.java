package com.liang.flink.basic.repair;

import lombok.Data;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Data
public class RepairState {
    private int subtaskId;
    private Roaring64Bitmap bitmap;
}
