package com.liang.repair.impl;

import com.liang.common.service.filesystem.ObsWriter;
import com.liang.repair.trait.Runner;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ObsWriterTest implements Runner {
    @SneakyThrows
    @Override
    public void run(String[] args) throws Exception {
        ObsWriter writer = new ObsWriter("obs://hadoop-obs/flink/tb1/");
        //writer.enableCache();
        ArrayList<String> list = new ArrayList<>();
        for (int i = 1; i <= 102400; i++) {
            writer.update(UUID.randomUUID().toString());
            TimeUnit.SECONDS.sleep(5);
            //list.add(UUID.randomUUID().toString() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID());
        }
        //writer.update(list);
        //writer.update(list);
        //writer.update(list);
    }
}
