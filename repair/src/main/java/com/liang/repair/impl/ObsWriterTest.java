package com.liang.repair.impl;

import com.liang.common.service.ObsWriter;
import com.liang.repair.trait.Runner;

public class ObsWriterTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        ObsWriter writer = new ObsWriter("obs://hadoop-obs/flink/tb1");
        writer.enableCache(1000 * 10);
        writer.println("test content1");
        writer.println("test content2");
        writer.println("test content3");
        writer.println("test content4");
        writer.println("test content5");
    }
}
