package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisEnhancer;
import com.liang.repair.trait.Runner;

import java.util.List;
import java.util.Set;

public class LocalTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        RedisEnhancer redis = new RedisEnhancer("localhost");
        Set<String> res = redis.exec(j -> j.keys("*"));
        for (String re : res) {
            System.out.println(re + " -> " + redis.exec(j -> j.get(re)));
        }

        JdbcTemplate jdbcTemplate = new JdbcTemplate("localhost");
        List<String> showTables = jdbcTemplate.queryForList("show tables", resultSet -> resultSet.getString(1));
        System.out.println(showTables);
    }
}
