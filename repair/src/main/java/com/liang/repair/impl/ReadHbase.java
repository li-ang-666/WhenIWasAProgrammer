package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

public class ReadHbase extends ConfigHolder {
    public static void main(String[] args) {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
        HbaseSchema hbaseSchema = HbaseSchema.builder()
                .namespace("prism_c")
                .tableName("human_all_count")
                .columnFamily("cf")
                .rowKeyReverse(false)
                .build();
        HbaseOneRow row = hbaseTemplate.getRow(new HbaseOneRow(hbaseSchema, "J0FL2MH02Q9Z6BCV6"));
        log.info("row: {}", JsonUtils.toString(row));
    }
}
