package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommonTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");

        HbaseSchema hbaseSchema = new HbaseSchema("test", "data_concat", "cf1", true);

        HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, "2318455639")
                .put("aaa", "1")
                .put("bbb", "2")
                .put("ccc", "3");

        hbaseTemplate.upsert(hbaseOneRow);
    }
}
