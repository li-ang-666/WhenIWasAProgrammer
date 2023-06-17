package com.liang.repair.impl;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommonTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        DorisTemplate dorisCluster = new DorisTemplate("dorisCluster");
        DorisSchema dorisSchema = DorisSchema.builder()
                .database("test_db")
                .tableName("stream_load_test")
                .uniqueDeleteOn("id = 3")
                .uniqueOrderBy("__DORIS_SEQUENCE_COL__")
                .build();
        DorisOneRow dorisOneRow = new DorisOneRow(dorisSchema)
                .put("id", 1)
                .put("__DORIS_SEQUENCE_COL__", "101")
                .put("name", "987654321");
        dorisCluster.load(dorisOneRow);
    }
}
