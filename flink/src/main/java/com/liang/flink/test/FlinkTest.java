package com.liang.flink.test;

import cn.hutool.core.lang.Snowflake;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) {
        new Snowflake(1);
        new Snowflake(2);
        new Snowflake(3);
        new Snowflake(4);
        new Snowflake(5);
        new Snowflake(6);
        new Snowflake(7);
        new Snowflake(8);
        new Snowflake(9);
        new Snowflake(0);
        new Snowflake(11);
        new Snowflake(12);
        new Snowflake(13);
        new Snowflake(14);
        new Snowflake(15);
        new Snowflake(16);
        new Snowflake(17);
        new Snowflake(18);
        new Snowflake(19);
        new Snowflake(20);
        new Snowflake(2100000000000L);
    }
}