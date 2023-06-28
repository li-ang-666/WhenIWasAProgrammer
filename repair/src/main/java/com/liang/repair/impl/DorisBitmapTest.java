package com.liang.repair.impl;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.trait.Runner;

import java.util.Collections;

public class DorisBitmapTest implements Runner {

    @Override
    public void run(String[] args) throws Exception {
        JdbcTemplate doris = new JdbcTemplate("doris");
        DorisTemplate dorisTemplate = new DorisTemplate("dorisSink");
        DorisSchema dorisSchema = DorisSchema.builder()
                .database("test_db")
                .tableName("bitmap_test")
                .derivedColumns(Collections.singletonList("btmp = to_bitmap(btmp)"))
                .build();
        DorisOneRow row1 = new DorisOneRow(dorisSchema)
                .put("id", 1)
                .put("btmp", null);
        DorisOneRow row2 = new DorisOneRow(dorisSchema)
                .put("id", 1)
                .put("btmp", 243);
        DorisOneRow row3 = new DorisOneRow(dorisSchema)
                .put("id", 1)
                .put("btmp", 287667876573L);
        DorisOneRow row4 = new DorisOneRow(dorisSchema)
                .put("id", 1)
                .put("btmp", 2222222222L);
        dorisTemplate.update(row1, row2, row3,row4);
    }
}
