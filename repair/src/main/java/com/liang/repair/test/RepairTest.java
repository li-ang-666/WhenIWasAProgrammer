package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        log.info("");
        Roaring64Bitmap emptyCompany = new Roaring64Bitmap();
        new JdbcTemplate("116.prism")
                .streamQuery(true,
                        "select graph_id from entity_empty_index where entity_type = 2 and deleted = 0",
                        rs -> emptyCompany.add(rs.getLong(1)));
        log.info("");
    }
}
