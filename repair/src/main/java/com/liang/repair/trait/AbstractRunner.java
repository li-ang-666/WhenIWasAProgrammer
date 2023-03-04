package com.liang.repair.trait;

import com.liang.common.database.template.JdbcTemplate;
import com.liang.common.database.template.JedisTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRunner implements Runner {
    /*----------子类便携变量---------*/
    protected Logger log = LoggerFactory.getLogger(this.getClass());

    /*----------子类便携方法----------*/
    protected JdbcTemplate jdbc(String name) {
        return new JdbcTemplate(name);
    }

    protected JedisTemplate jedis(String name) {
        return new JedisTemplate(name);
    }
}
