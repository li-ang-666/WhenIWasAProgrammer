package com.liang.repair.impl;

import com.liang.common.database.template.JdbcTemplate;
import com.liang.repair.trait.AbstractRunner;

import java.util.HashMap;
import java.util.List;

public class Attr extends AbstractRunner {
    @Override
    public void run(String[] args) throws Exception {
        HashMap<String, Object> map = new HashMap<>();
        JdbcTemplate abs1 = jdbc("abs1");
        List<String> tables = abs1.queryForList("show tables like '%dwd_hcm%'", rs -> rs.getString(1));
        for (String table : tables) {
            List<String> columns = abs1.queryForList("desc " + table, rs -> rs.getString(1));
            for (String column : columns) {
                if (column.matches("hcm_biz_model_attribute_id")) {
                    map.put(table, column);
                }
            }
        }
        map.forEach((k, v) -> {
            System.out.println(k + " -> " + v);
        });
    }
}
