package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.repair.trait.Runner;

import java.util.ArrayList;

public class HbaseTemplateTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink")
                .enableCache(5000);

        HbaseSchema schema1 = HbaseSchema.builder()
                .namespace("test")
                .tableName("data_concat")
                .rowKeyReverse(false)
                .columnFamily("cf1")
                .build();

        HbaseSchema schema2 = HbaseSchema.builder()
                .namespace("test")
                .tableName("data_concat_offline")
                .rowKeyReverse(false)
                .columnFamily("cf1")
                .build();

        HbaseOneRow row1 = new HbaseOneRow(schema1, "111")
                .put("id", 1)
                .put("name", "aaa");

        HbaseOneRow row2 = new HbaseOneRow(schema2, "111")
                .put("id", 2)
                .put("name", "bbb");

        ArrayList<HbaseOneRow> hbaseOneRows = new ArrayList<>();
        for (int i = 1; i <= 1024; i++) {
            hbaseOneRows.add(row1);
        }

        hbaseTemplate.upsertImmediately(row1, row2);
        hbaseTemplate.upsertImmediately(hbaseOneRows);
    }
}
