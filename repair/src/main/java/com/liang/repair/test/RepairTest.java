package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.concurrent.TimeUnit;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        log.info("1");
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        JdbcTemplate jdbcTemplate = new JdbcTemplate("104.data_bid");
        Thread thread = new Thread(() -> {
            jdbcTemplate.streamQuery(true, "select id from company_bid", rs -> {
            });
        });
        thread.setDaemon(true);
        thread.start();
        TimeUnit.SECONDS.sleep(5);
        thread.interrupt();
        log.info("1");
    }
}
