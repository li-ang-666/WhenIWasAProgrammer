package com.liang.repair.impl.cache;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.repair.service.ConfigHolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DorisTemplateTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        DorisTemplate dorisTemplate = new DorisTemplate("dorisSink");
        dorisTemplate.enableCache(5000, 1024);

        DorisSchema uniqueSchema = DorisSchema.builder()
                .database("test_db")
                .tableName("stream_load_test")
                .uniqueDeleteOn("__DORIS_DELETE_SIGN__ = 1")
                .uniqueOrderBy("__DORIS_SEQUENCE_COL__")
                .derivedColumns(Arrays.asList("id = id + 10", "name = concat('name - ',name)"))
                .build();

        DorisSchema aggSchema = DorisSchema.builder()
                .database("test_db")
                .tableName("agg_test")
                .derivedColumns(Collections.singletonList("id = id+100"))
                .build();


        DorisOneRow row1 = new DorisOneRow(uniqueSchema)
                .put("id", "1")
                .put("name", "Jackk")
                .put("__DORIS_DELETE_SIGN__", 0)
                .put("__DORIS_SEQUENCE_COL__", System.currentTimeMillis());

        DorisOneRow row2 = new DorisOneRow(aggSchema)
                .put("id", "1")
                .put("name", "Andy");

        ArrayList<DorisOneRow> dorisOneRows = new ArrayList<>();
        for (int i = 1; i <= 1024; i++) {
            dorisOneRows.add(row1);
        }

        dorisTemplate.update(row1, row2);
        dorisTemplate.update(dorisOneRows);
        TimeUnit.SECONDS.sleep(10);
    }
}
