package com.liang.flink.test;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) throws Exception {
        System.out.println(new BigDecimal("0.5").compareTo(new BigDecimal("0.500000")));
    }
}
