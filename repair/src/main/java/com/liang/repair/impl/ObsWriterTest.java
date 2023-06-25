package com.liang.repair.impl;

import com.liang.common.service.filesystem.ObsWriter;
import com.liang.repair.trait.Runner;

import java.util.ArrayList;
import java.util.UUID;

public class ObsWriterTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        ObsWriter writer = new ObsWriter("obs://hadoop-obs/flink/tb1/");
        writer.enableCache(1500, (int) (50000 * 50 * 1.2) - 1);
        ArrayList<String> list = new ArrayList<>();
        for (int i = 1; /*i <= 50000 * 50 * 1.2*/ ; i++) {
            writer.update(UUID.randomUUID().toString());
            //list.add(UUID.randomUUID().toString());
        }
        //writer.update(list);
        //writer.update(list);
        //writer.update(list);
    }
}
