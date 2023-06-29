package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.trait.Runner;

import java.util.List;
import java.util.Map;

public class DorisBitmapTest implements Runner {

    @Override
    public void run(String[] args) throws Exception {
        JdbcTemplate doris = new JdbcTemplate("doris");
        doris.update("INSERT INTO bitmap_test VALUES\n" +
                " (0, bitmap_empty()),\n" +
                " (1, to_bitmap(243)),\n" +
                " (2, bitmap_from_array([1,2,3,4,5,434543])),\n" +
                " (3, to_bitmap(287667876573)),\n" +
                " (4, bitmap_from_array([487667876573, 387627876573, 987567876573, 187667876573]))");

        doris.update("set global return_object_data_as_binary=true");
        List<Map<String, Object>> columnMaps = doris.queryForColumnMaps("select id,btmp bitmap from bitmap_test");
        System.out.println(columnMaps);
    }
}