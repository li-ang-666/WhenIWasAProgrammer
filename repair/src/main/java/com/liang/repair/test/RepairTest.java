package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception ignore) {
            }
            running.set(false);
        }).start();
        JdbcTemplate jdbcTemplate = new JdbcTemplate("435.company_base");
        String sql = "select 1 from company_index where create_time >= '2024-09-10 00:00:00' order by id";
        jdbcTemplate.streamQueryInterruptible(sql, running, rs -> {
        });
        System.out.println(111);
    }
}
