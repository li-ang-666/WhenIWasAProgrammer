package com.liang.flink.dto;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.commons.beanutils.BeanUtils;

import java.util.concurrent.ConcurrentLinkedQueue;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SubRepairTask extends RepairTask {
    private ConcurrentLinkedQueue<SingleCanalBinlog> pendingQueue = new ConcurrentLinkedQueue<>();
    private volatile long currentId;
    private long targetId;

    @SneakyThrows // BeanUtils.copyProperties()
    public SubRepairTask(RepairTask repairTask) {
        BeanUtils.copyProperties(this, repairTask);
    }
}
