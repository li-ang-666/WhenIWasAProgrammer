package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class RepairSplit implements Serializable {
    private RepairTask repairTask;
    private List<Long> ids;
}
