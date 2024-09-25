package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class RepairSplit implements Serializable {
    private RepairTask repairTask;
    private Roaring64Bitmap ids;
}
