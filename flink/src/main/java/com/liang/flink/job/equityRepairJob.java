package com.liang.flink.job;

import com.liang.flink.service.LocalConfigFile;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@LocalConfigFile("equity-repair.yml")
public class equityRepairJob {
    public static void main(String[] args) throws Exception {
        String file = equityRepairJob.class.getAnnotation(LocalConfigFile.class).value();
        String[] fileArg = new String[]{file};

        EquityDirectJob.main(fileArg);
        Thread.sleep(5000);

        EquityBfsJob.main(fileArg);
        Thread.sleep(1000);

        EquityTotalJob.main(fileArg);
        Thread.sleep(1000);

        EquityControlJob.main(fileArg);
    }
}
