package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        log.info("1");
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        JdbcTemplate jdbcTemplate = new JdbcTemplate("116.prism");
        jdbcTemplate.streamQuery(false, "select id from equity_ratio", rs -> {
            bitmap.add(rs.getLong("id"));
        });
        log.info("1");
    }
}
