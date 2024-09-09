package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.Serializable;

@Data
public class RepairState implements Serializable {
    private RepairTask repairTask;
    private Roaring64Bitmap bitmap = new Roaring64Bitmap();
}
