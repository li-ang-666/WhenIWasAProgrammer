package com.liang.repair.impl;

import com.liang.common.service.database.template.MemJdbcTemplate;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class CommonTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        MemJdbcTemplate jdbcTemplate = new MemJdbcTemplate("aaaaa");
        jdbcTemplate.update("create table test(id int)");
        jdbcTemplate.update("insert into test values(100),(200),(333),(456),(700)");
        List<String> res = jdbcTemplate.queryForList("select group_concat(t1.id order by cast(id as int) desc separator '-') from test t1 join test t2 on t1.id = t2.id", rs -> rs.getString(1));
        for (String result : res) {
            log.info("-----------{}", result);
        }
        jdbcTemplate.update("drop table if exists 李昂牛逼");
    }
}
