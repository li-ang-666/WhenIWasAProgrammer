package com.liang.repair.impl;

import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.filesystem.ObsWriter;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

public class ScanHbase extends ConfigHolder {
    public static void main(String[] args) {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
        ObsWriter obsWriter = new ObsWriter("obs://hadoop-obs/flink/hbase/");
        obsWriter.enableCache();
        hbaseTemplate.scan(HbaseSchema.HUMAN_ALL_COUNT, row -> obsWriter.update(JsonUtils.toString(row)));
    }
}
