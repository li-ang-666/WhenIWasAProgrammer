package com.liang.repair.impl;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.repair.trait.Runner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class DorisTemplateTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        DorisTemplate dorisTemplate = new DorisTemplate("dorisSink")
        .enableCache(1000 * 10);

        DorisSchema uniqueSchema = DorisSchema.builder()
                .database("test_db")
                .tableName("stream_load_test")
                .uniqueDeleteOn("__DORIS_DELETE_SIGN__ = 1")
                .uniqueOrderBy("__DORIS_SEQUENCE_COL__")
                .derivedColumns(Arrays.asList("id = id + 10", "name = concat('name - ',name)"))
                .build();

        DorisOneRow row1 = new DorisOneRow(uniqueSchema)
                .put("id", "1")
                .put("name", "Jackk")
                .put("__DORIS_DELETE_SIGN__", 0)
                .put("__DORIS_SEQUENCE_COL__", System.currentTimeMillis());

        DorisOneRow row2 = new DorisOneRow(uniqueSchema)
                .put("id", "2")
                .put("name", "Jsonn")
                .put("__DORIS_DELETE_SIGN__", 0)
                .put("__DORIS_SEQUENCE_COL__", System.currentTimeMillis());

        DorisOneRow row3 = new DorisOneRow(uniqueSchema)
                .put("id", "3")
                .put("name", "Tomm")
                .put("__DORIS_DELETE_SIGN__", 0)
                .put("__DORIS_SEQUENCE_COL__", System.currentTimeMillis());

        DorisSchema aggSchema = DorisSchema.builder()
                .database("test_db")
                .tableName("agg_test")
                .derivedColumns(Collections.singletonList("id = id+100"))
                .build();
        DorisOneRow row4 = new DorisOneRow(aggSchema)
                .put("id", "1")
                .put("name", "Andy");

        dorisTemplate.load(row1, row2, row3, row4);
        ArrayList<DorisOneRow> dorisOneRows = new ArrayList<>();
        for (int i = 1; i <= 10240; i++) {
            dorisOneRows.add(row1);
        }
        dorisTemplate.load(dorisOneRows);
    }
}
