package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.repair.service.ConfigHolder;

public class CommonTest extends ConfigHolder {
    public static void main(String[] args) {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");

        HbaseSchema hbaseSchema = new HbaseSchema("test", "data_concat", "cf1", true);

        HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, "2318455639")
                .put("aaa", "1")
                .put("bbb", "2")
                .put("ccc", "3");

        hbaseTemplate.update(hbaseOneRow);
    }
}
