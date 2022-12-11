package com.liang.repair.impl;

import com.liang.common.database.template.JdbcTemplate;
import com.liang.repair.trait.AbstractRunner;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DorisNew extends AbstractRunner {
    @Override
    public void run(String[] args) throws Exception {
        JdbcTemplate template = jdbc("dorisproduction2");
        while (true) {
            template.queryForList("select count(1) from dwd_users", rs -> rs.getString(1))
                    .forEach(e -> log.info(e));
            List<String> list2 = template.queryForList("show frontends;", rs -> {
                if ("Yes".equals(rs.getString("CurrentConnected"))) {
                    log.info("CurrentConnected: {}\n\n", rs.getString(2));
                }
                return null;
            });
            TimeUnit.SECONDS.sleep(3);
        }
    }
}
