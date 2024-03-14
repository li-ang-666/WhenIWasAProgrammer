package com.liang.repair.impl.cache;

import com.liang.common.service.storage.ObsWriter;
import com.liang.repair.service.ConfigHolder;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

public class ObsWriterTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        ObsWriter writer = new ObsWriter("obs://hadoop-obs/flink/tb1/");
        writer.enableCache(50000, 10240);
        String row = StringUtils.repeat(UUID.randomUUID().toString(), 10);
        for (int i = 1; i <= 1024000000; i++) {
            writer.update(row);
        }

        Thread.sleep(1000 * 3600);
    }
}
