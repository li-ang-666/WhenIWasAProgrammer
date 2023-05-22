package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.repair.trait.Runner;

import java.util.HashMap;

public class CommonTest implements Runner {

    @Override
    public void run(String[] args) throws Exception {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("test");
        HbaseOneRow hbaseOneRow = new HbaseOneRow("test", "student", "2",
                new HashMap<String, Object>() {{
                    put("cf1:name", "a");
                    put("cf1:addr", "b");
                    put("cf1:phone", "123");
                    put("cf1:info", "你好");
                }});
        hbaseTemplate.upsert(hbaseOneRow);

        HbaseOneRow hbaseOneRow2 = new HbaseOneRow("test", "student", "2",
                new HashMap<String, Object>());
        hbaseTemplate.delete(hbaseOneRow);
    }
}
