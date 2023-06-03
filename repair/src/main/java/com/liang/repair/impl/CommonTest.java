package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
public class CommonTest implements Runner {

    @Override
    public void run(String[] args) throws Exception {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("localhost");
        jdbcTemplate.update("show tables");
        jdbcTemplate.batchUpdate(Collections.singletonList("show tables"));
        jdbcTemplate.batchUpdate(Collections.singletonList("show table"));
        jdbcTemplate.queryForObject("show tables", rs -> rs.getString(1));
        jdbcTemplate.queryForList("show tables", rs -> rs.getString(1));
        jdbcTemplate.queryForColumnMaps("show tables");

        RedisTemplate redisTemplate = new RedisTemplate("localhost");
        redisTemplate.scan();
        redisTemplate.set("aaa", "aaa");
        redisTemplate.get("aaa");
        redisTemplate.del("aaa");
        redisTemplate.hScan("first_hash");

        HbaseTemplate hbaseTemplate = new HbaseTemplate("test");
        hbaseTemplate.upsert(new HbaseOneRow("dataConcat", "-1")
                .put("aaa", "aaa"));
        hbaseTemplate.deleteRow(new HbaseOneRow("dataConcat", "-1"));
    }
}
