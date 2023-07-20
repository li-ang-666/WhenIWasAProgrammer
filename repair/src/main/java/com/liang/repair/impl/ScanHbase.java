package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.filesystem.ObsWriter;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

public class ScanHbase extends ConfigHolder {
    public static void main(String[] args) {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
        ObsWriter obsWriter = new ObsWriter("obs://hadoop-obs/flink");
        obsWriter.enableCache();
        HbaseOneRow queryRow = new HbaseOneRow(HbaseSchema.HUMAN_ALL_COUNT, "J0FL2MH02Q9Z6BCV6");
        hbaseTemplate.scan(queryRow, row -> obsWriter.update(JsonUtils.toString(row)));
    }
}
