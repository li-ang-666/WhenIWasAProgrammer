package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.repair.trait.Runner;

public class CommonTest implements Runner {

    @Override
    public void run(String[] args) throws Exception {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("test");
        HbaseOneRow hbaseOneRow = new HbaseOneRow("dataConcat", "-1");
        hbaseOneRow.put("id", 1).put("name", "alex");
        hbaseTemplate.upsert(hbaseOneRow);
    }
}
