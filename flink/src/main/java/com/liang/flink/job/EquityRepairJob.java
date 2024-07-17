package com.liang.flink.job;

import com.liang.flink.service.LocalConfigFile;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@LocalConfigFile("equity-repair.yml")
public class EquityRepairJob {
    public static void main(String[] args) throws Exception {
        String file = EquityRepairJob.class.getAnnotation(LocalConfigFile.class).value();
        String[] fileArg = new String[]{file};
        EquityDirectJob.main(fileArg);
        EquityBfsJob.main(fileArg);
        EquityTotalJob.main(fileArg);
        EquityTagJob.main(fileArg);
        EquityControlJob.main(fileArg);
    }
}
