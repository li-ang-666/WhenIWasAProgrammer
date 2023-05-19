package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisEnhancer;
import com.liang.repair.annotation.Prop;
import com.liang.repair.trait.Runner;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;

@Prop(key1 = "aaa", key2 = "bbb")
public class LocalTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        RedisEnhancer redis = new RedisEnhancer("localhost");
        Map<String, String> firstHash = redis.hScan("first_hash");
        System.out.println(firstHash);

        JdbcTemplate jdbcTemplate = new JdbcTemplate("localhost");
        List<Tuple2<String, String>> query = jdbcTemplate.queryForList("select * from liang_test", rs -> Tuple2.of(rs.getString(1), rs.getString(2)));
        for (Tuple2<String, String> tuple2 : query) {
            System.out.println(tuple2.f0 + " -> " + tuple2.f1);
        }
    }
}
