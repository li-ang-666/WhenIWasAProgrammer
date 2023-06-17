package com.liang.repair.impl;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;

@Slf4j
public class CommonTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        DorisTemplate dorisCluster = new DorisTemplate("dorisCluster");
        HashMap<String, Object> columnMap1 = new HashMap<String, Object>() {{
            put("id", 1);
            put("name", "aaa");
            put("__DORIS_DELETE_SIGN__", "0");
            put("__DORIS_SEQUENCE_COL__", 1);
        }};
        DorisOneRow dorisOneRow = new DorisOneRow(new DorisSchema("test_db", "stream_load_test", "__DORIS_DELETE_SIGN__ = 1", "__DORIS_SEQUENCE_COL__", Collections.singletonList("name=concat('name-',name)")));
        dorisOneRow.putAll(columnMap1);
        dorisCluster.load(dorisOneRow);
        HashMap<String, Object> columnMap2 = new HashMap<String, Object>() {{
            put("id", 2);
            put("name", "bbb");
            put("__DORIS_DELETE_SIGN__", "0");
            put("__DORIS_SEQUENCE_COL__", 1);
        }};
        dorisOneRow = new DorisOneRow(new DorisSchema("test_db", "stream_load_test", "__DORIS_DELETE_SIGN__ = 1", "__DORIS_SEQUENCE_COL__", Collections.singletonList("name=concat('name-',name)")));
        dorisOneRow.putAll(columnMap2);
        dorisCluster.load(dorisOneRow);
        HashMap<String, Object> columnMap3 = new HashMap<String, Object>() {{
            put("id", 3);
            put("name", "ccc");
            put("__DORIS_DELETE_SIGN__", "0");
            put("__DORIS_SEQUENCE_COL__", 1);
        }};
        dorisOneRow = new DorisOneRow(new DorisSchema("test_db", "stream_load_test", "__DORIS_DELETE_SIGN__ = 1", "__DORIS_SEQUENCE_COL__", Collections.singletonList("name=concat('name-',name)")));
        dorisOneRow.putAll(columnMap3);
        dorisCluster.load(dorisOneRow);
    }
}
