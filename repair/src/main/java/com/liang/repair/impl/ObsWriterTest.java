package com.liang.repair.impl;

import com.liang.common.service.filesystem.ObsWriter;
import com.liang.repair.service.ConfigHolder;

import java.util.ArrayList;
import java.util.UUID;

public class ObsWriterTest extends ConfigHolder {
    public static void main(String[] args) {
        ObsWriter writer = new ObsWriter("obs://hadoop-obs/flink/tb1/");
        writer.enableCache();
        String row = UUID.randomUUID().toString();
        ArrayList<String> rows = new ArrayList<>();
        for (int i = 1; i <= 10240; i++) {
            rows.add(row);
        }
        writer.update(row);
        writer.update(rows);
    }
}
