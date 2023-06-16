package com.liang.repair.impl;

import com.liang.common.service.database.template.DorisTemplate;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CommonTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        DorisTemplate dorisCluster = new DorisTemplate("dorisCluster");
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        HashMap<String, Object> columnMap1 = new HashMap<String, Object>() {{
            put("id", 1);
            put("name", "aaa");
            put("__DORIS_DELETE_SIGN__", "0");
            put("__DORIS_SEQUENCE_COL__", 14);
        }};
        HashMap<String, Object> columnMap2 = new HashMap<String, Object>() {{
            put("id", 1);
            put("name", "bbb");
            put("__DORIS_DELETE_SIGN__", "1");
            put("__DORIS_SEQUENCE_COL__", 15);
        }};
        list.add(columnMap1);
        list.add(columnMap2);
        dorisCluster.streamLoad("test", list);
    }
}
