package com.liang.repair.impl.cache;

import cn.hutool.core.util.SerializeUtil;
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
                .tableName("unique_test")
                .uniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON)
                .derivedColumns(Arrays.asList("id = id + 10", "name = concat('name - ',name)"))
                .build();

        DorisSchema aggSchema = DorisSchema.builder()
                .database("test_db")
                .tableName("agg_test")
                .derivedColumns(Collections.singletonList("id = id + 100"))
                .build();


        DorisOneRow unique = new DorisOneRow(uniqueSchema)
                .put("id", 0)
                .put("name", "Jack")
                .put("__DORIS_DELETE_SIGN__", 0);

        DorisOneRow agg = new DorisOneRow(aggSchema)
                .put("id", "1")
                .put("name", "Andy");

        ArrayList<DorisOneRow> dorisOneRows = new ArrayList<>();
        for (int i = 1; i <= 1024; i++) {
            DorisOneRow clone = SerializeUtil.clone(unique);
            clone.put("id", i);
            dorisOneRows.add(clone);
        }

        dorisTemplate.update(unique, agg);
        dorisTemplate.update(dorisOneRows);
        TimeUnit.SECONDS.sleep(5);
    }
}
