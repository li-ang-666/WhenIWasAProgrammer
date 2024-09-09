package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Data
public class RepairState {
    private RepairTask repairTask;
    private Roaring64Bitmap bitmap = new Roaring64Bitmap();
}
