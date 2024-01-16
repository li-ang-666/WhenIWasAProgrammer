package com.liang.repair.impl.cache;

import cn.hutool.core.util.SerializeUtil;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.repair.service.ConfigHolder;

import java.util.Arrays;

public class DorisTemplateTest2 extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        DorisSchema uniqueSchema = DorisSchema.builder()
                .database("test_db")
                .tableName("unique_test")
                .uniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON)
                .derivedColumns(Arrays.asList("id = id + 10", "name = concat('name - ', name)"))
                .build();
        DorisTemplate dorisTemplate = new DorisTemplate("dorisSink", uniqueSchema);
        DorisOneRow unique = new DorisOneRow(uniqueSchema)
                .put("id", 2222222222L)
                .put("name", "UNIQUE")
                .put("__DORIS_DELETE_SIGN__", 0);
        for (int i = 1; i <= 1024 * 1024 * 1024; i++) {
            DorisOneRow clone = SerializeUtil.clone(unique);
            clone.put("id", i);
            if (!dorisTemplate.cacheBatch(clone.getColumnMap())) {
                dorisTemplate.flushBatch();
            }
        }
        dorisTemplate.flushBatch();
    }
}
