package com.liang.flink.dto;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentLinkedQueue;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SubRepairTask extends RepairTask {
    private final ConcurrentLinkedQueue<SingleCanalBinlog> pendingQueue = new ConcurrentLinkedQueue<>();
    private volatile long currentId;
    private volatile long targetId;

    @SneakyThrows({IllegalAccessException.class, InvocationTargetException.class})
    public SubRepairTask(RepairTask repairTask) {
        BeanUtils.copyProperties(this, repairTask);
    }
}
