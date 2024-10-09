package com.liang.repair.test;

import com.liang.common.dto.config.RepairTask;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        RepairTask repairTask1 = new RepairTask();
        RepairTask repairTask2 = new RepairTask();
        System.out.println(repairTask1.equals(repairTask2));
    }
}
