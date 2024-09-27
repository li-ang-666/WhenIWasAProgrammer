package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        long time1 = System.currentTimeMillis() / 1000;
        JdbcTemplate jdbcTemplate = new JdbcTemplate("104.data_bid");
        AtomicInteger atomicInteger = new AtomicInteger(0);
        jdbcTemplate.streamQuery(false, "select id from company_bid", rs -> {
            atomicInteger.set(atomicInteger.get() + 1);
            if (atomicInteger.get() % 10000 == 0) {
                System.out.println(atomicInteger.get());
            }
        });
    }
}
